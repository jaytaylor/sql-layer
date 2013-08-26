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

import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.util.MultipleCauseException;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

import static com.foundationdb.server.service.session.Session.Key;
import static com.foundationdb.server.service.session.Session.StackKey;

public class FDBTransactionService implements TransactionService {
    private static final Logger LOG = LoggerFactory.getLogger(FDBTransactionService.class);

    private static final Key<TransactionState> TXN_KEY = Key.named("TXN_KEY");
    private static final Key<Boolean> ROLLBACK_KEY = Key.named("TXN_ROLLBACK");
    private static final StackKey<Callback> PRE_COMMIT_KEY = StackKey.stackNamed("TXN_PRE_COMMIT");
    private static final StackKey<Callback> AFTER_END_KEY = StackKey.stackNamed("TXN_AFTER_END");
    private static final StackKey<Callback> AFTER_COMMIT_KEY = StackKey.stackNamed("TXN_AFTER_COMMIT");
    private static final StackKey<Callback> AFTER_ROLLBACK_KEY = StackKey .stackNamed("TXN_AFTER_ROLLBACK");
    private static final String CONFIG_COMMIT_AFTER_MILLIS = "fdbsql.fdb.periodicallCommit.afterMillis";
    private static final String CONFIG_COMMIT_AFTER_BYTES = "fdbsql.fdb.periodicallCommit.afterBytes";

    private final FDBHolder fdbHolder;
    private final ConfigurationService configService;
    private static long commitAfterMillis, commitAfterBytes;

    @Inject
    public FDBTransactionService(FDBHolder fdbHolder,
                                 ConfigurationService configService) {
        this.fdbHolder = fdbHolder;
        this.configService = configService;
    }

    public static class TransactionState {
        final Transaction transaction;
        long startTime;
        long bytesSet;

        public TransactionState(Transaction transaction) {
            this.transaction = transaction;
            reset();
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setBytes(byte[] key, byte[] value) {
            transaction.set(key, value);
            bytesSet += value.length;
        }

        public void reset() {
            this.startTime = System.currentTimeMillis();
            this.bytesSet = 0;
        }

        public boolean timeToCommit() {
            long dt = System.currentTimeMillis() - startTime;
            if ((dt > commitAfterMillis) ||
                (bytesSet > commitAfterBytes)) {
                LOG.debug("Commit after {} ms. / {} bytes", dt, bytesSet);
                return true;
            }
            return false;
        }
    }

    public TransactionState getTransaction(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        return txn;
    }

    public void setRollbackPending(Session session) {
        session.put(ROLLBACK_KEY, Boolean.TRUE);
    }


    //
    // Service
    //

    @Override
    public void start() {
        commitAfterMillis = Long.parseLong(configService.getProperty(CONFIG_COMMIT_AFTER_MILLIS));
        commitAfterBytes = Long.parseLong(configService.getProperty(CONFIG_COMMIT_AFTER_BYTES));
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // TransactionService
    //

    @Override
    public boolean isTransactionActive(Session session) {
        TransactionState txn = getTransactionInternal(session);
        return (txn != null);
    }

    @Override
    public boolean isRollbackPending(Session session) {
        return session.get(ROLLBACK_KEY) == Boolean.TRUE;
    }

    @Override
    public long getTransactionStartTimestamp(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        return txn.getTransaction().getReadVersion().get();
    }

    @Override
    public void beginTransaction(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireInactive(txn); // No nesting
        txn = new TransactionState(fdbHolder.getDatabase().createTransaction());
        session.put(TXN_KEY, txn);
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
            public boolean commitOrRetry() {
                return commitTransactionInternal(session, true);
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
        if(isRollbackPending(session)) {
            throw new IllegalStateException("Rollback is pending");
        }
        commitTransactionInternal(session, false);
    }

    @Override
    public boolean commitOrRetryTransaction(Session session) {
        if(isRollbackPending(session)) {
            throw new IllegalStateException("Rollback is pending");
        }
        return commitTransactionInternal(session, true);
    }

    protected boolean commitTransactionInternal(Session session, boolean retry) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        RuntimeException re = null;
        try {
            long startTime = txn.getTransaction().getReadVersion().get();
            runCallbacks(session, PRE_COMMIT_KEY, startTime, null);
            txn.getTransaction().commit().get();
            long commitTime = txn.getTransaction().getCommittedVersion();
            runCallbacks(session, AFTER_COMMIT_KEY, commitTime, null);
        } catch(RuntimeException e1) {
            if (retry) {
                try {
                    txn.getTransaction().onError(e1).get();
                    // Getting here means retry.
                    clearStack(session, AFTER_COMMIT_KEY);
                    clearStack(session, AFTER_ROLLBACK_KEY);
                    clearStack(session, AFTER_END_KEY);
                    return true;
                }
                catch (RuntimeException e2) {
                    re = e2;
                }
            }
            else {
                re = e1;
            }
        } finally {
            end(session, txn, re);
        }
        return false;
    }

    @Override
    public void rollbackTransaction(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        RuntimeException re = null;
        try {
            runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
        } catch(RuntimeException e) {
            re = e;
        } finally {
            end(session, txn, re);
        }
    }

    @Override
    public void rollbackTransactionIfOpen(Session session) {
        TransactionState txn = getTransactionInternal(session);
        if(txn != null) {
            rollbackTransaction(session);
        }
    }

    @Override
    public int getTransactionStep(Session session) {
        // TODO
        return 0;
    }

    @Override
    public int setTransactionStep(Session session, int newStep) {
        // TODO
        return 0;
    }

    @Override
    public int incrementTransactionStep(Session session) {
        // TODO
        return 0;
    }


    @Override
    public void periodicallyCommit(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        if (txn.timeToCommit()) {
            // TODO: Better to leave callbacks until user-initiated commit?
            long startTime = txn.getTransaction().getReadVersion().get();
            runCallbacks(session, PRE_COMMIT_KEY, startTime, null);
            txn.getTransaction().commit().get();
            long commitTime = txn.getTransaction().getCommittedVersion();
            runCallbacks(session, AFTER_COMMIT_KEY, commitTime, null);
            txn.getTransaction().reset();
            txn.reset();
        }
    }

    @Override
    public void addCallback(Session session, CallbackType type, Callback callback) {
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void addCallbackOnActive(Session session, CallbackType type, Callback callback) {
        requireActive(getTransactionInternal(session));
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void addCallbackOnInactive(Session session, CallbackType type, Callback callback) {
        requireInactive(getTransactionInternal(session));
        session.push(getCallbackKey(type), callback);
    }


    public <T> T runTransaction (Function<Transaction,T> retryable) throws Exception {
        return fdbHolder.getDatabase().run(retryable);
    }
    
    //
    // Helpers
    //

    private TransactionState getTransactionInternal(Session session) {
        return session.get(TXN_KEY);
    }

    private void requireInactive(TransactionState txn) {
        if(txn != null) {
            throw new IllegalStateException("Transaction already began");
        }
    }

    private void requireActive(TransactionState txn) {
        if(txn == null) {
            throw new IllegalStateException("No transaction open");
        }
    }

    private void end(Session session, TransactionState txn, RuntimeException cause) {
        RuntimeException re = cause;
        // if txn != null, Transaction gets aborted. Abnormal end, though, so no calling of rollback hooks.
        try {
            session.remove(TXN_KEY);
            // TODO: Keep and reset() instead?
            if(txn != null) {
                txn.getTransaction().dispose();
            }
        } catch(RuntimeException e) {
            re = MultipleCauseException.combine(re, e);
        } finally {
            session.remove(ROLLBACK_KEY);
            clearStack(session, PRE_COMMIT_KEY);
            clearStack(session, AFTER_COMMIT_KEY);
            clearStack(session, AFTER_ROLLBACK_KEY);
            runCallbacks(session, AFTER_END_KEY, -1, re);
        }
    }

    private void clearStack(Session session, Session.StackKey<Callback> key) {
        Deque<Callback> stack = session.get(key);
        if(stack != null) {
            stack.clear();
        }
    }

    private void runCallbacks(Session session, Session.StackKey<Callback> key, long timestamp, RuntimeException cause) {
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

    private static Session.StackKey<Callback> getCallbackKey(CallbackType type) {
        switch(type) {
            case PRE_COMMIT:    return PRE_COMMIT_KEY;
            case COMMIT:        return AFTER_COMMIT_KEY;
            case ROLLBACK:      return AFTER_ROLLBACK_KEY;
            case END:           return AFTER_END_KEY;
        }
        throw new IllegalArgumentException("Unknown CallbackType: " + type);
    }
}

