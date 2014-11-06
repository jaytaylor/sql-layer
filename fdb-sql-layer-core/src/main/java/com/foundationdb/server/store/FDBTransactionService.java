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
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.FDBCommitUnknownResultException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.util.MultipleCauseException;
import com.foundationdb.KeySelector;
import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.Random;

import static com.foundationdb.server.service.session.Session.Key;
import static com.foundationdb.server.service.session.Session.StackKey;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   transactionCheck/
 * </pre>
 *
 * <p>
 *     The above directory is used to determine whether a 
 *     transaction that failed with <code>commit_unknown_result</code> actually succeeded.
 *     The keys are unique to a session and begin with a millisecond timestamp to allow
 *     for garbage collection. The values are monotonic counters.
 * </p>
 */
public class FDBTransactionService implements TransactionService {
    private static final Logger LOG = LoggerFactory.getLogger(FDBTransactionService.class);

    private static final Key<TransactionState> TXN_KEY = Key.named("TXN_KEY");
    private static final Key<Boolean> ROLLBACK_KEY = Key.named("TXN_ROLLBACK");
    private static final Key<FDBPendingIndexChecks.CheckTime> CONSTRAINT_CHECK_TIME_KEY = Key.named("CONSTRAINT_CHECK_TIME");
    private static final Key<TransactionCheckCounter> TXN_CHECK_KEY = Key.named("TXN_CHECK_KEY");
    private static final StackKey<Callback> PRE_COMMIT_KEY = StackKey.stackNamed("TXN_PRE_COMMIT");
    private static final StackKey<Callback> AFTER_END_KEY = StackKey.stackNamed("TXN_AFTER_END");
    private static final StackKey<Callback> AFTER_COMMIT_KEY = StackKey.stackNamed("TXN_AFTER_COMMIT");
    private static final StackKey<Callback> AFTER_ROLLBACK_KEY = StackKey .stackNamed("TXN_AFTER_ROLLBACK");
    private static final String CONFIG_COMMIT_AFTER_MILLIS = "fdbsql.fdb.periodically_commit.after_millis";
    private static final String CONFIG_COMMIT_AFTER_BYTES = "fdbsql.fdb.periodically_commit.after_bytes";
    private static final String CONFIG_COMMIT_SCAN_LIMIT = "fdbsql.fdb.periodically_commit.scan_limit";
    private static final String CONFIG_READ_AHEAD_DISABLE = "fdbsql.fdb.xact.read_ahead_disable";
    private static final String CONFIG_READ_YOUR_WRITES_DISABLE = "fdbsql.fdb.xact.read_your_writes_disable";
    private static final String UNIQUENESS_CHECKS_METRIC = "SQLLayerUniquenessPending";

    private static final List<String> TRANSACTION_CHECK_DIR_PATH = Arrays.asList("transactionCheck");

    private final FDBHolder fdbHolder;
    private final ConfigurationService configService;
    private final MetricsService metricsService;
    private long commitAfterMillis, commitAfterBytes;
    private int commitScanLimit;
    private boolean readAheadDisable, readYourWritesDisable;
    private LongMetric uniquenessChecksMetric;
    private byte[] packedTransactionCheckPrefix;

    @Inject
    public FDBTransactionService(FDBHolder fdbHolder,
                                 ConfigurationService configService,
                                 MetricsService metricsService) {
        this.fdbHolder = fdbHolder;
        this.configService = configService;
        this.metricsService = metricsService;
    }

    public class TransactionState {
        final Transaction transaction;
        FDBPendingIndexChecks indexChecks;
        long startTime;
        long bytesSet;
        public long uniquenessTime;
        Map<ForeignKey,Boolean> deferredForeignKeys;
        boolean forceImmediateForeignKeyCheck;
        final Session session;

        public TransactionState(FDBPendingIndexChecks.CheckTime checkTime, Session session) {
            this.transaction = fdbHolder.getDatabase().createTransaction();
            if (readAheadDisable)
                transaction.options().setReadAheadDisable();
            if (readYourWritesDisable)
                transaction.options().setReadYourWritesDisable();
            if ((checkTime != null) && checkTime.isDelayed())
                this.indexChecks = new FDBPendingIndexChecks(checkTime,
                                                             uniquenessChecksMetric);
            reset();
            this.session = session;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public long getStartTime() {
            return startTime;
        }

        public Future<byte[]> getFuture(byte[] key) {
                return transaction.get(key);
        }

        public byte[] getValue(byte[] key) {
            try {
                return getFuture(key).get();
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        
        public byte[] getSnapshotValue(byte[] key) {
            try {
                return transaction.snapshot().get(key).get();
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        
        public Future<List<KeyValue>> getSnapshotRangeAsFutureList (byte[] start, byte[] end, int limit, boolean reverse) {
            return transaction.snapshot().getRange(start, end, limit, reverse).asList();
        }
        
        public Future<List<KeyValue>> getRangeAsFutureList(byte[] start, byte[] end, int limit) {
            return transaction.getRange(start, end, limit).asList();
        }
        
        public List<KeyValue> getRangeAsValueList(byte[] start, byte[] end) {
            try {
                return getRangeAsFutureList(start, end, Transaction.ROW_LIMIT_UNLIMITED).get();
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }

        public AsyncIterator<KeyValue> getSnapshotRangeIterator(KeySelector start, KeySelector end, int limit, boolean reverse) {
            return transaction.snapshot().getRange(start, end, limit, reverse).iterator();
        }

        public AsyncIterator<KeyValue> getRangeIterator(KeySelector start, KeySelector end, int limit, boolean reverse) {
            return transaction.getRange(start, end, limit, reverse).iterator();
        }
        
        public AsyncIterator<KeyValue> getRangeIterator(byte[] start, byte[] end) {
            return transaction.getRange(start, end, Transaction.ROW_LIMIT_UNLIMITED, false).iterator();
        }

        public AsyncIterable<KeyValue> getRangeIterator(Range range, int limit) {
            return transaction.getRange(range, limit);
        }

        public boolean getRangeExists (Range range, int limit) {
            try {
                return transaction.getRange(range, limit).iterator().hasNext();
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        
        public void setBytes(byte[] key, byte[] value) {
            try {
                transaction.set(key, value);
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
            bytesSet += key.length;
            bytesSet += value.length;
        }

        public void clearKey (byte[] key) {
            try {
                transaction.clear(key); 
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        
        public void clearRange(byte[] start, byte[] end) {
            try {
                transaction.clear(start, end); 
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }

        public void clearRange(Range range) {
            try {
                transaction.clear(range); 
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        
        public void mutate (MutationType type, byte[] key, byte[] value) {
            try {
                transaction.mutate(type, key, value);
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }

        public FDBPendingIndexChecks getIndexChecks(boolean create) {
            if (create && (indexChecks == null))
                indexChecks = new FDBPendingIndexChecks(FDBPendingIndexChecks.CheckTime.IMMEDIATE,
                                                        uniquenessChecksMetric);

            return indexChecks;
        }

        public void reset() {
            this.startTime = System.currentTimeMillis();
            this.bytesSet = 0;
            this.forceImmediateForeignKeyCheck = false;
            if (indexChecks != null)
                indexChecks.clear();
        }

        public boolean timeToCommit() {
            long dt = System.currentTimeMillis() - startTime;
            if ((dt > commitAfterMillis) ||
                (bytesSet > commitAfterBytes)) {
                LOG.debug("Periodic commit after {} ms. / {} bytes", dt, bytesSet);
                return true;
            }
            return false;
        }

        /**
         * Commit transaction and reset for immediate reuse, as if {@link #commitTransaction}
         * and then {@link #beginTransaction} were called.
         *
         * <i>All</i> exceptions are propagated immediately and leave the state unchanged.
         */
        public void commitAndReset(Session session) {
            commitInternal(session, false, false);
            transaction.reset();
            reset();
        }

        public int periodicallyCommitScanLimit() {
            return commitScanLimit;
        }

        public boolean isDeferred(ForeignKey foreignKey) {
            return foreignKey.isDeferred(deferredForeignKeys);
        }

        public void setDeferredForeignKey(ForeignKey foreignKey, boolean deferred) {
            deferredForeignKeys = ForeignKey.setDeferred(deferredForeignKeys, foreignKey, deferred);
        }

        public boolean getForceImmediateForeignKeyCheck() {
            return forceImmediateForeignKeyCheck;
        }

        public boolean setForceImmediateForeignKeyCheck(boolean force) {
            boolean old = this.forceImmediateForeignKeyCheck;
            this.forceImmediateForeignKeyCheck = force;
            return old;
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

    public byte[] dirPathPrefix(List<String> dirPath) {
        return fdbHolder.getRootDirectory().createOrOpen(fdbHolder.getDatabase(), dirPath).get().pack();
    }

    //
    // Service
    //

    @Override
    public void start() {
        commitAfterMillis = Long.parseLong(configService.getProperty(CONFIG_COMMIT_AFTER_MILLIS));
        commitAfterBytes = Long.parseLong(configService.getProperty(CONFIG_COMMIT_AFTER_BYTES));
        commitScanLimit =  Integer.parseInt(configService.getProperty(CONFIG_COMMIT_SCAN_LIMIT));
        readAheadDisable = Boolean.parseBoolean(configService.getProperty(CONFIG_READ_AHEAD_DISABLE));
        readYourWritesDisable = Boolean.parseBoolean(configService.getProperty(CONFIG_READ_YOUR_WRITES_DISABLE));
        uniquenessChecksMetric = metricsService.addLongMetric(UNIQUENESS_CHECKS_METRIC);
        packedTransactionCheckPrefix = dirPathPrefix(TRANSACTION_CHECK_DIR_PATH);
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
        txn = new TransactionState(session.get(CONSTRAINT_CHECK_TIME_KEY), session);
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
        commitInternal(session, false, true);
    }

    @Override
    public boolean commitOrRetryTransaction(Session session) {
        if(isRollbackPending(session)) {
            throw new IllegalStateException("Rollback is pending");
        }
        return commitInternal(session, true, true);
    }

    protected boolean commitInternal(Session session, boolean retry, boolean clearState) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        boolean retried = false;
        RuntimeException re = null;
        try {
            if (txn.getIndexChecks(false) != null) {
                txn.getIndexChecks(false).performChecks(session, txn, FDBPendingIndexChecks.CheckPass.TRANSACTION);
            }
            if (LOG.isDebugEnabled()) {
                long dt = System.currentTimeMillis() - txn.startTime;
                long ut = Math.round(txn.uniquenessTime / 1.0e6);
                LOG.debug("Commit after {} ms. / {} ms. uniqueness", dt, ut);
            }
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
                    runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
                    txn.reset();
                    clearState = false;
                    retried = true;
                }
                catch (RuntimeException e2) {
                    re = FDBAdapter.wrapFDBException(session, e2);
                }
            }
            else {
                if (clearState) {
                    runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
                }
                re = FDBAdapter.wrapFDBException(session, e1);
            }
        } finally {
            end(session, txn, clearState, re);
        }
        return retried;
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
            end(session, txn, true, re);
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
    public boolean periodicallyCommit(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        if (txn.timeToCommit()) {
            txn.commitAndReset(session);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean shouldPeriodicallyCommit(Session session) {
        TransactionState txn = getTransactionInternal(session);
        requireActive(txn);
        return txn.timeToCommit();
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
        for(int tries = 1; ; ++tries) {
            try {
                beginTransaction(session);
                T value = callable.call();
                commitTransaction(session);
                return value;
            } catch(InvalidOperationException e) {
                if(!e.getCode().isRollbackClass()) {
                    throw e;
                }
                // Back-off, via onError(), is already provided in commitTransaction[Internal]
                LOG.debug("Retry attempt {} due to rollback", tries, e);
            } catch(RuntimeException e) {
                throw  FDBAdapter.wrapFDBException(session, e);
            } catch(Exception e) {
                throw new AkibanInternalException("Unexpected Exception", e);
            } finally {
                rollbackTransactionIfOpen(session);
            }
        }
    }

    public <T> T runTransaction (Function<Transaction,T> retryable) {
        return fdbHolder.getDatabase().run(retryable);
    }
    
    @Override
    public void setSessionOption(Session session, SessionOption option, String value) {
        switch (option) {
        case CONSTRAINT_CHECK_TIME:
            FDBPendingIndexChecks.CheckTime checkTime = null;
            if (value != null) {
                value = value.toUpperCase();
                if (value.startsWith("DEFERRED")) // Allow old names for time being.
                    value = "DELAYED" + value.substring(8);
                try {
                    checkTime = FDBPendingIndexChecks.CheckTime.valueOf(value);
                }
                catch (IllegalArgumentException ex) {
                    throw new InvalidParameterValueException(ex.getMessage());
                }
            }
            session.put(CONSTRAINT_CHECK_TIME_KEY, checkTime);
            break;
        }
    }

    @Override
    public int markForCheck(Session session) {
        try {
            TransactionState txn = getTransaction(session);
            TransactionCheckCounter counter = session.get(TXN_CHECK_KEY);
            if (counter == null) {
                do {
                    counter = new TransactionCheckCounter();
                } while (txn.transaction.get(transactionCheckKey(counter)).get() != null);
                session.put(TXN_CHECK_KEY, counter);
            }
            int result = ++counter.counter;
            txn.transaction.set(transactionCheckKey(counter), Tuple2.from(result).pack());
            return result;
        } 
        catch (Exception ex) {
            throw FDBAdapter.wrapFDBException(session, ex);
        }
    }

    @Override
    public boolean checkSucceeded(Session session, Exception retryException,
                                  int sessionCounter) {
        if ((sessionCounter < 0) || 
            !(retryException instanceof FDBCommitUnknownResultException)) {
            return false;
        }
        try {
            TransactionState txn = getTransaction(session);
            TransactionCheckCounter counter = session.get(TXN_CHECK_KEY);
            byte[] stored = txn.transaction.get(transactionCheckKey(counter)).get();
            if (stored == null) {
                return false;
            }
            return (sessionCounter == Tuple2.fromBytes(stored).getLong(0));
        } 
        catch (Exception ex) {
            throw FDBAdapter.wrapFDBException(session, ex);
        }
    }

    @Override
    public void setDeferredForeignKey(Session session, ForeignKey foreignKey, boolean deferred) {
        TransactionState txn = getTransaction(session);
        txn.setDeferredForeignKey(foreignKey, deferred);
    }

    @Override
    public void checkStatementForeignKeys(Session session) {
        TransactionState txn = getTransaction(session);
        if (txn.getIndexChecks(false) != null) {
            txn.getIndexChecks(false).performChecks(session, txn, FDBPendingIndexChecks.CheckPass.STATEMENT);
        }
    }

    @Override
    public boolean getForceImmediateForeignKeyCheck(Session session) {
        TransactionState txn = getTransaction(session);
        return txn.getForceImmediateForeignKeyCheck();
    }

    @Override
    public boolean setForceImmediateForeignKeyCheck(Session session, boolean force) {
        TransactionState txn = getTransaction(session);
        return txn.setForceImmediateForeignKeyCheck(force);
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

    private void end(Session session, TransactionState txn, boolean clearState, RuntimeException cause) {
        RuntimeException re = cause;
        try {
            if(clearState) {
                TransactionState sessionTxn = session.remove(TXN_KEY);
                assert sessionTxn == txn;
                if(txn != null) {
                    txn.getTransaction().dispose();
                }
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

    static class TransactionCheckCounter {
        static final Random random = new Random();
        final long timestamp = System.currentTimeMillis();
        final int unique = random.nextInt();
        int counter = 0;
    }

    public byte[] transactionCheckKey(TransactionCheckCounter counter) {
        return ByteArrayUtil.join(packedTransactionCheckPrefix,
                                  Tuple2.from(counter.timestamp, counter.unique).pack());
    }
}
