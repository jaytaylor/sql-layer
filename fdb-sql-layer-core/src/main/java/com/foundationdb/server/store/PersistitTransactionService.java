/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.sql.parser.IsolationLevel;
import com.foundationdb.util.MultipleCauseException;
import com.google.inject.Inject;
import com.persistit.SessionId;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException; 
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.concurrent.Callable;

import static com.foundationdb.server.service.session.Session.Key;
import static com.foundationdb.server.service.session.Session.StackKey;

public class PersistitTransactionService implements TransactionService {
    private static final Logger LOG = LoggerFactory.getLogger(PersistitTransactionService.class);

    private static final long NO_START_MILLIS = -1;
    private static final String CONFIG_COMMIT_AFTER_MILLIS = "fdbsql.persistit.periodically_commit.after_millis";

    private static final Key<Transaction> TXN_KEY = Key.named("TXN_KEY");
    private static final Key<Long> START_MILLIS_KEY = Key.named("TXN_START_MILLIS");
    private static final Key<PersistitDeferredForeignKeys> DEFERRED_FOREIGN_KEYS_KEY = Key.named("DEFERRED_FOREIGN_KEYS");
    private static final Key<Boolean> IMMEDIATE_FOREIGN_KEY_CHECK_KEY = Key.named("IMMEDIATE_FOREIGN_KEYS_CHECK");
    private static final StackKey<Callback> PRE_COMMIT_KEY = StackKey.stackNamed("TXN_PRE_COMMIT");
    private static final StackKey<Callback> AFTER_END_KEY = StackKey.stackNamed("TXN_AFTER_END");
    private static final StackKey<Callback> AFTER_COMMIT_KEY = StackKey.stackNamed("TXN_AFTER_COMMIT");
    private static final StackKey<Callback> AFTER_ROLLBACK_KEY = StackKey.stackNamed("TXN_AFTER_ROLLBACK");

    private final ConfigurationService configService;
    private final TreeService treeService;
    private long commitAfterMillis;

    @Inject
    public PersistitTransactionService(ConfigurationService configService, TreeService treeService) {
        this.configService = configService;
        this.treeService = treeService;
    }

    @Override
    public boolean isTransactionActive(Session session) {
        Transaction txn = getTransaction(session);
        return (txn != null) && txn.isActive();
    }

    @Override
    public boolean isRollbackPending(Session session) {
        Transaction txn = getTransaction(session);
        return (txn != null) && txn.isRollbackPending();
    }

    @Override
    public long getTransactionStartTimestamp(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        return txn.getStartTimestamp();
    }

    @Override
    public void beginTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireInactive(txn); // Do not want to use Persistit nesting
        try {
            txn.begin();
            if(commitAfterMillis != NO_START_MILLIS) {
                session.put(START_MILLIS_KEY, System.currentTimeMillis());
            }
        } catch(PersistitException e) {
            PersistitAdapter.handlePersistitException(session, e);
        }
    }

    @Override
    public CloseableTransaction beginCloseableTransaction(final Session session) {
        beginTransaction(session);
        return new CloseableTransaction() {
            @Override
            public void commit() {
                commitTransaction(session);
            }

            @Override
            public void rollback() {
                rollbackTransaction(session);
            }

            @Override
            public void close() {
                rollbackTransactionIfOpen(session);
            }
        };
    }

    @Override
    public void commitTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        commitInternal(session, txn, false, true);
    }

    @Override
    public boolean commitOrRetryTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        return commitInternal(session, txn, true, true);
    }

    @Override
    public void rollbackTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        RuntimeException re = null;
        try {
            txn.rollback();
            runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
        } catch(RuntimeException e) {
            re = e;
        } finally {
            end(session, txn, true, re);
        }
    }

    @Override
    public void rollbackTransactionIfOpen(Session session) {
        Transaction txn = getTransaction(session);
        if((txn != null) && txn.isActive()) {
            rollbackTransaction(session);
        }
    }

    @Override
    public boolean periodicallyCommit(Session session) {
        if (shouldPeriodicallyCommit(session)) {
            commitTransaction(session);
            beginTransaction(session);
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldPeriodicallyCommit(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        if(commitAfterMillis != NO_START_MILLIS) {
            long startMillis = session.get(START_MILLIS_KEY);
            long dt = System.currentTimeMillis() - startMillis;
            if(dt > commitAfterMillis) {
                LOG.debug("Periodic commit after {} ms", dt);
                return true;
            }
        }
        return false;
    }

    @Override
    public void addCallback(Session session, CallbackType type, Callback callback) {
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void addCallbackOnActive(Session session, CallbackType type, Callback callback) {
        requireActive(getTransaction(session));
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void addCallbackOnInactive(Session session, CallbackType type, Callback callback) {
        requireInactive(getTransaction(session));
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void run(Session session, final Runnable runnable) {
        run(session, new Callable<Void>() {
            @Override
            public Void call() {
                runnable.run();
                return null;
            }
        });
    }

    @Override
    public <T> T run(Session session, Callable<T> callable) {
        Transaction oldTransaction = getTransaction(session);
        SessionId oldSessionId = null;
        Long oldStartMillis = null;
        if ((oldTransaction != null) &&
            // Anything that would prevent begin() from working.
            (oldTransaction.isActive() ||
             oldTransaction.isRollbackPending() ||
             oldTransaction.isCommitted())) {
            oldSessionId = treeService.getDb().getSessionId();
            treeService.getDb().setSessionId(new SessionId());
            session.remove(TXN_KEY);
            oldStartMillis = session.remove(START_MILLIS_KEY);
        }
        try {
            for(int tries = 1; ; ++tries) {
                try {
                    beginTransaction(session);
                    T value = callable.call();
                    commitTransaction(session);
                    return value;
                } catch(InvalidOperationException e) {
                    if(e.getCode().isRollbackClass()) {
                        LOG.debug("Retry attempt {} due to rollback", tries, e);
                    } else {
                        throw e;
                    }
                } catch(RuntimeException e) {
                    throw e;
                } catch(Exception e) {
                    throw new AkibanInternalException("Unexpected Exception", e);
                } finally {
                    rollbackTransactionIfOpen(session);
                }
                // TODO: Back-off?
            }
        }
        finally {
            if (oldSessionId != null) {
                treeService.getDb().setSessionId(oldSessionId);
                session.put(TXN_KEY, oldTransaction);
                session.put(START_MILLIS_KEY, oldStartMillis);
            }
        }
    }

    @Override
    public void setSessionOption(Session session, SessionOption option, String value) {
        // No specific handling.
    }

    @Override
    public int markForCheck(Session session) {
        return -1;
    }

    @Override
    public boolean checkSucceeded(Session session, Exception retryException, int sessionCounter) {
        return false;
    }

    @Override
    public void setDeferredForeignKey(Session session, ForeignKey foreignKey, boolean deferred) {
        getDeferredForeignKeys(session, true).setDeferredForeignKey(foreignKey, deferred);
    }

    @Override
    public void checkStatementForeignKeys(Session session) {
        PersistitDeferredForeignKeys deferred = getDeferredForeignKeys(session, false);
        if (deferred != null)
            deferred.checkStatementForeignKeys(session);
    }

    @Override
    public boolean getForceImmediateForeignKeyCheck(Session session) {
        requireActive(getTransaction(session));
        Boolean current = session.get(IMMEDIATE_FOREIGN_KEY_CHECK_KEY);
        return (current == null) ? false : current;
    }

    @Override
    public boolean setForceImmediateForeignKeyCheck(Session session, boolean force) {
        requireActive(getTransaction(session));
        Boolean prev = session.put(IMMEDIATE_FOREIGN_KEY_CHECK_KEY, force);
        return (prev == null) ? false : prev;
    }

    protected PersistitDeferredForeignKeys getDeferredForeignKeys(Session session, boolean create) {
        PersistitDeferredForeignKeys deferred = session.get(DEFERRED_FOREIGN_KEYS_KEY);
        if ((deferred != null) || !create)
            return deferred;
        deferred = new PersistitDeferredForeignKeys();
        session.put(DEFERRED_FOREIGN_KEYS_KEY, deferred);
        addCallback(session, CallbackType.PRE_COMMIT, RUN_DEFERRED_FOREIGN_KEYS);
        addCallback(session, CallbackType.END, CLEAR_DEFERRED_FOREIGN_KEYS);
        return deferred;
    }

    protected final Callback RUN_DEFERRED_FOREIGN_KEYS = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            PersistitDeferredForeignKeys deferred = session.get(DEFERRED_FOREIGN_KEYS_KEY);
            deferred.checkTransactionForeignKeys(session);
        }
    };

    protected final Callback CLEAR_DEFERRED_FOREIGN_KEYS = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(DEFERRED_FOREIGN_KEYS_KEY);
        }
    };

    @Override
    public IsolationLevel actualIsolationLevel(IsolationLevel level) {
        return IsolationLevel.SNAPSHOT_ISOLATION_LEVEL;
    }

    @Override
    public IsolationLevel setIsolationLevel(Session session, IsolationLevel level) {
        // Ignored.
        return IsolationLevel.SNAPSHOT_ISOLATION_LEVEL;
    }

    @Override
    public boolean isolationLevelRequiresReadOnly(Session session) {
        return false;
    }

    @Override
    public void start() {
        // None
        commitAfterMillis = Long.parseLong(configService.getProperty(CONFIG_COMMIT_AFTER_MILLIS));
        if(commitAfterMillis < 0) {
            commitAfterMillis = NO_START_MILLIS;
        }
    }

    @Override
    public void stop() {
        // None
    }

    @Override
    public void crash() {
        // None
    }

    private Transaction getTransaction(Session session) {
        Transaction txn = session.get(TXN_KEY); // Note: Assumes 1 session per thread
        if(txn == null) {
            txn = treeService.getDb().getTransaction();
            session.put(TXN_KEY, txn);
        }
        return txn;
    }

    private void requireInactive(Transaction txn) {
        if((txn != null) && txn.isActive()) {
            throw new IllegalStateException("Transaction already began");
        }
    }

    private void requireActive(Transaction txn) {
        if((txn == null) || !txn.isActive()) {
            throw new IllegalStateException("No transaction open");
        }
    }

    private boolean commitInternal(Session session, Transaction txn, boolean retry, boolean clearState) {
        boolean retried = false;
        RuntimeException re = null;
        try {
            runCallbacks(session, PRE_COMMIT_KEY, txn.getStartTimestamp(), null);
            txn.commit();
            runCallbacks(session, AFTER_COMMIT_KEY, txn.getCommitTimestamp(), null);
        } catch(RollbackException e) {
            if(retry) {
                runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
                clearState = false;
                retried = true;
            } else {
                re = PersistitAdapter.wrapPersistitException(session, e);
            }
        } catch(PersistitException e) {
            re = PersistitAdapter.wrapPersistitException(session, e);
            if (clearState) {
                runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
            }
        } finally {
            end(session, txn, clearState, re);
        }
        return retried;
    }

    private void end(Session session, Transaction txn, boolean clearState, RuntimeException cause) {
        RuntimeException re = cause;
        try {
            if(clearState && txn.isActive() && !txn.isCommitted() && !txn.isRollbackPending()) {
                txn.rollback(); // Abnormally ended, do not call rollback hooks
            }
        } catch(RuntimeException e) {
            re = MultipleCauseException.combine(re, e);
        }
        try {
            if(clearState) {
                txn.end();
                //session.remove(TXN_KEY); // Needed if Sessions ever move between threads
            }
        } catch(RuntimeException e) {
            re = MultipleCauseException.combine(re, e);
        } finally {
            session.remove(IMMEDIATE_FOREIGN_KEY_CHECK_KEY);
            clearStack(session, PRE_COMMIT_KEY);
            clearStack(session, AFTER_COMMIT_KEY);
            clearStack(session, AFTER_ROLLBACK_KEY);
            runCallbacks(session, AFTER_END_KEY, -1, re);
        }
    }

    private void clearStack(Session session, StackKey<Callback> key) {
        Deque<Callback> stack = session.get(key);
        if(stack != null) {
            stack.clear();
        }
    }

    private void runCallbacks(Session session, StackKey<Callback> key, long timestamp, RuntimeException cause) {
        RuntimeException exceptions = cause;
        Callback cb;
        while((cb = session.pop(key)) != null) {
            try {
                cb.run(session, timestamp);
            } catch(RuntimeException e) {
                exceptions = MultipleCauseException.combine(exceptions, e);
            }
        }
        if(exceptions != null) {
            throw exceptions;
        }
    }

    private static StackKey<Callback> getCallbackKey(CallbackType type) {
        switch(type) {
            case PRE_COMMIT:    return PRE_COMMIT_KEY;
            case COMMIT:        return AFTER_COMMIT_KEY;
            case ROLLBACK:      return AFTER_ROLLBACK_KEY;
            case END:           return AFTER_END_KEY;
        }
        throw new IllegalArgumentException("Unknown CallbackType: " + type);
    }
}
