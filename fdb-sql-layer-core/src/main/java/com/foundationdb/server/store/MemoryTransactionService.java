/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.LockTimeoutException;
import com.foundationdb.server.error.NoTransactionInProgressException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.error.TransactionAbortedException;
import com.foundationdb.server.error.TransactionInProgressException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.Session.Key;
import com.foundationdb.server.service.session.Session.StackKey;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.sql.parser.IsolationLevel;
import com.foundationdb.util.MultipleCauseException;
import com.foundationdb.util.Strings;
import com.google.common.primitives.UnsignedBytes;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KV storage (via TreeMap<byte[],byte[]) and transaction provider.
 *
 * Per-key locking providing repeatable read semantics. No gap locking.
 *
 * gets() take read (shared) locks, sets() and clears() take write (exclusive) locks
 * and shared can be upgraded to exclusive 'optimistically' via tag (StampedLock-ish).
 *
 * Uncommitted writes go directly into master KV after (exclusive) lock is acquired and an
 * undo log is maintained to restore proper state on rollback.
 *
 * Lock acquisition has an aggressive (1s) timeout after which a retryable error is thrown.
 */
public class MemoryTransactionService implements TransactionService
{
    private static final Logger LOG = LoggerFactory.getLogger(MemoryTransactionService.class);

    private static final int PERIODIC_COMMIT_MILLS = 500;
    private static final int PERIODIC_COMMIT_BYTES = 100000;
    private static final int LOCK_TIMEOUT_MILLIS = 1 * 1000;
    private static final Key<MemoryTransactionImpl> TXN_KEY = Key.named("TXN");
    private static final StackKey<Callback> PRE_COMMIT_KEY = StackKey.stackNamed("TXN_PRE_COMMIT");
    private static final StackKey<Callback> AFTER_END_KEY = StackKey.stackNamed("TXN_AFTER_END");
    private static final StackKey<Callback> AFTER_COMMIT_KEY = StackKey.stackNamed("TXN_AFTER_COMMIT");
    private static final StackKey<Callback> AFTER_ROLLBACK_KEY = StackKey .stackNamed("TXN_AFTER_ROLLBACK");

    private final ConcurrentMap<BytesHolder,TaggedLock> locks;
    private final KVMap db;

    @Inject
    public MemoryTransactionService() {
        this.locks = new ConcurrentHashMap<>();
        this.db = new KVMap();
    }

    //
    // Service
    //

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        locks.clear();
        synchronized(db) {
            db.clear();
        }
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
        return (session.get(TXN_KEY) != null);
    }

    @Override
    public boolean isRollbackPending(Session session) {
        return getTransactionInternal(session).isRollbackPending;
    }

    @Override
    public long getTransactionStartTimestamp(Session session) {
        return getTransactionInternal(session).startMillis;
    }

    @Override
    public void beginTransaction(Session session) {
        if(isTransactionActive(session)) {
            throw new TransactionInProgressException();
        }
        MemoryTransactionImpl txn = new MemoryTransactionImpl(session);
        session.put(TXN_KEY, txn);
    }

    @Override
    public CloseableTransaction beginCloseableTransaction(final Session session) {
        beginTransaction(session);
        return new CloseableTransaction()
        {
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
            throw new TransactionAbortedException();
        }
        commitInternal(session, false, true);
    }

    @Override
    public boolean commitOrRetryTransaction(Session session) {
        if(isRollbackPending(session)) {
            throw new TransactionAbortedException();
        }
        return commitInternal(session, true, true);
    }

    @Override
    public void rollbackTransaction(Session session) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        RuntimeException re = null;
        try {
            rollbackInternal(session, txn);
        } catch(RuntimeException e) {
            re = e;
        } finally {
            end(session, txn, true, re);
        }
    }

    @Override
    public void rollbackTransactionIfOpen(Session session) {
        if(isTransactionActive(session)) {
            rollbackTransaction(session);
        }
    }

    @Override
    public boolean periodicallyCommit(Session session) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        if(txn.isTimeToCommit()) {
            commitInternal(session, false, false);
            txn.reset();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean shouldPeriodicallyCommit(Session session) {
        return getTransactionInternal(session).isTimeToCommit();
    }

    @Override
    public void addCallback(Session session, CallbackType type, Callback callback) {
        session.push(getCallbackKey(type), callback);
    }

    @Override
    public void addCallbackOnActive(Session session, CallbackType type, Callback callback) {
        if(!isTransactionActive(session)) {
            throw new IllegalStateException("Expected active");
        }
        addCallback(session, type, callback);
    }

    @Override
    public void addCallbackOnInactive(Session session, CallbackType type, Callback callback) {
        if(isTransactionActive(session)) {
            throw new IllegalStateException("Expected inactive");
        }
        addCallback(session, type, callback);
    }

    @Override
    public void run(Session session, final Runnable runnable) {
        run(session, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
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
                // Back-off?
                LOG.debug("Retrying callable, attempt {}", tries);
            } catch(RuntimeException e) {
                throw e;
            } catch(Exception e) {
                throw new AkibanInternalException("Unexpected Exception", e);
            } finally {
                rollbackTransactionIfOpen(session);
            }
        }
    }

    @Override
    public void setSessionOption(Session session, SessionOption option, String value) {
        // None
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
        MemoryTransactionImpl txn = getTransactionInternal(session);
        txn.deferredForeignKeys = ForeignKey.setDeferred(txn.deferredForeignKeys, foreignKey, deferred);
    }

    @Override
    public void checkStatementConstraints(Session session) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        if(txn.pendingChecks != null) {
            txn.pendingChecks.performChecks(session, txn, MemoryIndexChecks.CheckPass.STATEMENT);
        }
    }

    @Override
    public boolean getForceImmediateForeignKeyCheck(Session session) {
        // Only called via Online DDL
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setForceImmediateForeignKeyCheck(Session session, boolean force) {
        // Only called via Online DDL
        throw new UnsupportedOperationException();
    }

    @Override
    public IsolationLevel actualIsolationLevel(IsolationLevel level) {
        return IsolationLevel.REPEATABLE_READ_ISOLATION_LEVEL;
    }

    @Override
    public IsolationLevel setIsolationLevel(Session session, IsolationLevel level) {
        // Ignored.
        return IsolationLevel.REPEATABLE_READ_ISOLATION_LEVEL;
    }

    @Override
    public boolean isolationLevelRequiresReadOnly(Session session, boolean commitNow) {
        return false;
    }

    //
    // TreeMapTransactionImplService
    //

    public void addPendingCheck(Session session, MemoryIndexChecks.IndexCheck check) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        if(txn.pendingChecks == null) {
            txn.pendingChecks = new MemoryIndexChecks.PendingChecks();
        }
        txn.pendingChecks.add(session, txn, check);
    }

    public MemoryTransaction getTransaction(Session session) {
        return getTransactionInternal(session);
    }

    public boolean isDeferred(Session session, ForeignKey foreignKey) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        return foreignKey.isDeferred(txn.deferredForeignKeys);
    }

    public void setRollbackPending(Session session) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        txn.isRollbackPending = true;
    }

    //
    // Static
    //

    /** Wrapper class providing equals() and hashCode() for byte[] */
    private static class BytesHolder
    {
        public final byte[] bytes;

        private BytesHolder(byte[] bytes) {
            assert bytes != null;
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }
            if((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            BytesHolder that = (BytesHolder)o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static class CopiedEntry implements Entry<byte[], byte[]>
    {
        private final byte[] key;
        private final byte[] value;

        public CopiedEntry(Entry<byte[], byte[]> entry) {
            this.key = copy(entry.getKey());
            this.value = copy(entry.getValue());
        }

        @Override
        public byte[] getKey() {
            return key;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return Strings.hex(key) + "=" + Strings.hex(value);
        }
    }

    private static class KVMap extends TreeMap<byte[], byte[]>
    {
        private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

        private static final Comparator<byte[]> REVERSE_COMPARATOR = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return COMPARATOR.compare(o2, o1);
            }
        };

        public KVMap() {
            this(Collections.<byte[],byte[]>emptyMap(), false);
        }

        public KVMap(Map<byte[], byte[]> map, boolean reverse) {
            super(reverse ? REVERSE_COMPARATOR : COMPARATOR);
            putAll(map);
        }
    }

    private static class TaggedLock
    {
        private final ReadWriteLock rwLock;
        // ReadWriteLock does not support upgrading read to write.
        // Tag is checked pre-read.unlock() and must match post-write.lock().
        private final AtomicLong tag;

        public TaggedLock() {
            this.rwLock = new ReentrantReadWriteLock(true);
            this.tag = new AtomicLong(0);
        }

        public void readUnlock() {
            rwLock.readLock().unlock();
        }

        public void writeUnlock() {
            rwLock.writeLock().unlock();
        }

        public void readLock(Session session) {
            tryLock(session, rwLock.readLock(), "shared");
        }

        public void writeLock(Session session) {
            tryLock(session, rwLock.writeLock(), "exclusive");
            tag.incrementAndGet();
        }

        public void upgradeLock(Session session) {
            final long saveTag = tag.get();
            readUnlock();
            tryLock(session, rwLock.writeLock(), "upgrade");
            if(saveTag != tag.get()) {
                writeUnlock();
                throwTimeout(session, "optimistic upgrade");
            }
            tag.incrementAndGet();
        }

        private static void tryLock(Session session, Lock lock, String lockType) {
            try {
                if(!lock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    throwTimeout(session, lockType);
                }
            } catch(InterruptedException e) {
                throw new QueryCanceledException(session);
            }
        }

        private static void throwTimeout(Session session, String lockType) {
            LOG.trace("lock timeout: {}", lockType);
            // No telling where this happened, e.g. row could be half written.
            getTransactionInternal(session).isRollbackPending = true;
            throw new LockTimeoutException(LOCK_TIMEOUT_MILLIS, lockType, MemoryTransactionService.class.getSimpleName());
        }
    }

    private class MemoryTransactionImpl implements MemoryTransaction
    {
        final Session session;
        final List<UndoOp> undoLog;
        final Set<BytesHolder> readLocked;
        final Set<BytesHolder> writeLocked;

        long startMillis;
        long commitMillis;
        long bytesWritten;
        boolean isRollbackPending;
        Map<ForeignKey,Boolean> deferredForeignKeys;
        MemoryIndexChecks.PendingChecks pendingChecks;

        private MemoryTransactionImpl(Session session) {
            this.session = session;
            this.undoLog = new ArrayList<>();
            this.readLocked = new HashSet<>();
            this.writeLocked = new HashSet<>();
            reset();
        }

        private void readLock(byte[] rawKey) {
            BytesHolder key = new BytesHolder(rawKey);
            if(readLocked.contains(key) || writeLocked.contains(key)) {
                // Already locked
                return;
            }
            TaggedLock lock = locks.get(key);
            if(lock == null) {
                lock = new TaggedLock();
                TaggedLock prevLock = locks.putIfAbsent(key, lock);
                if(prevLock != null) {
                    lock = prevLock;
                }
            }
            lock.readLock(session);
            readLocked.add(key);
        }

        private void writeLock(byte[] rawKey) {
            BytesHolder key = new BytesHolder(rawKey);
            if(writeLocked.contains(key)) {
                // Already locked
                return;
            }
            TaggedLock lock = locks.get(key);
            if(lock == null) {
                lock = new TaggedLock();
                TaggedLock prevLock = locks.putIfAbsent(key, lock);
                if(prevLock != null) {
                    lock = prevLock;
                }
            }
            if(readLocked.contains(key)) {
                readLocked.remove(key);
                lock.upgradeLock(session);
            } else {
                lock.writeLock(session);
            }
            writeLocked.add(key);
        }

        public boolean isTimeToCommit() {
            if(bytesWritten > PERIODIC_COMMIT_BYTES) {
                return true;
            }
            if((System.currentTimeMillis() - startMillis) > PERIODIC_COMMIT_MILLS) {
                return true;
            }
            return false;
        }

        public void reset() {
            assert undoLog.isEmpty();
            assert readLocked.isEmpty();
            assert writeLocked.isEmpty();
            startMillis = System.currentTimeMillis();
            commitMillis = -1;
            bytesWritten = 0;
            isRollbackPending = false;
            if(deferredForeignKeys != null) {
                deferredForeignKeys.clear();
            }
            if(pendingChecks != null) {
                pendingChecks.clear();
            }
        }

        public void runUndo() {
            if(undoLog.isEmpty()) {
                return;
            }
            // Undo in reverse order than applied
            ListIterator<UndoOp> it = undoLog.listIterator(undoLog.size());
            synchronized(db) {
                while(it.hasPrevious()) {
                    UndoOp op = it.previous();
                    op.apply(db);
                }
            }
            undoLog.clear();
        }

        public void runUnlock() {
            // Release all locks
            for(BytesHolder key : readLocked) {
                TaggedLock lock = locks.get(key);
                lock.readUnlock();
            }
            readLocked.clear();
            for(BytesHolder key : writeLocked) {
                TaggedLock lock = locks.get(key);
                lock.writeUnlock();
            }
            writeLocked.clear();
        }

        //
        // TreeMapTransaction
        //

        @Override
        public byte[] get(byte[] key) {
            readLock(key);
            byte[] value;
            synchronized(db) {
                value = db.get(key);
            }
            return copy(value);
        }

        @Override
        public byte[] getUncommitted(byte[] key) {
            // No lock
            byte[] value;
            synchronized(db) {
                value = db.get(key);
            }
            return copy(value);
        }

        @Override
        public Iterator<Entry<byte[], byte[]>> getRange(byte[] beginKey, final byte[] endKey) {
            return getRange(beginKey, endKey, false);
        }

        @Override
        public Iterator<Entry<byte[], byte[]>> getRange(byte[] beginKey, byte[] endKey, boolean reverse) {
            // Duplicate as some consumers want to iterate while calling set/clear
            final KVMap subMap;
            synchronized(db) {
                subMap = new KVMap(db.subMap(beginKey, endKey), reverse);
            }
            for(byte[] key : subMap.keySet()) {
                readLock(key);
            }
            final Iterator<Entry<byte[], byte[]>> it = subMap.entrySet().iterator();
            return new Iterator<Entry<byte[], byte[]>>()  {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Entry<byte[], byte[]> next() {
                    Entry<byte[], byte[]> entry = it.next();
                    return new CopiedEntry(entry);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void set(byte[] key, byte[] value) {
            writeLock(key);
            byte[] k = copy(key);
            byte[] v = copy(value);
            byte[] prev;
            synchronized(db) {
                prev = db.put(k, v);
            }
            bytesWritten += key.length;
            bytesWritten += value.length;
            undoLog.add(new UndoOp(k, prev));
        }

        @Override
        public void clear(byte[] key) {
            writeLock(key);
            byte[] prev;
            synchronized(db) {
                prev = db.remove(key);
            }
            bytesWritten += key.length;
            undoLog.add(new UndoOp(copy(key), prev));
        }

        @Override
        public void clearRange(byte[] beginKey, byte[] endKey) {
            KVMap subMap;
            synchronized(db) {
                subMap = new KVMap(db.subMap(beginKey, endKey), false);
            }
            for(Entry<byte[], byte[]> entry : subMap.entrySet()) {
                writeLock(entry.getKey());
                undoLog.add(new UndoOp(entry.getKey(), entry.getValue()));
                bytesWritten += entry.getKey().length;
            }
            synchronized(db) {
                for(byte[] key : subMap.keySet()) {
                    db.remove(key);
                }
            }
        }
    }

    private static class UndoOp
    {
        private final byte[] key;
        private final byte[] value;

        private UndoOp(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public void apply(KVMap map) {
            if(value == null) {
                map.remove(key);
            } else {
                map.put(key, value);
            }
        }

        @Override
        public String toString() {
            return "UndoOp(" + Strings.hex(key) + "=" + (value == null ? null : Strings.hex(value)) + ")";
        }
    }


    private static void clearStack(Session session, Session.StackKey<Callback> key) {
        Deque<Callback> stack = session.get(key);
        if(stack != null) {
            stack.clear();
        }
    }

    private static boolean commitInternal(Session session, boolean allowRetry, boolean clearState) {
        MemoryTransactionImpl txn = getTransactionInternal(session);
        boolean shouldRetry = false;
        RuntimeException re = null;
        try {
            if(txn.pendingChecks != null) {
                txn.pendingChecks.performChecks(session, txn, MemoryIndexChecks.CheckPass.TRANSACTION);
            }
            runCallbacks(session, PRE_COMMIT_KEY, txn.startMillis, null);
            // Not much to do, locks are cleared in end().
            txn.undoLog.clear();
            txn.commitMillis = System.currentTimeMillis();
            runCallbacks(session, AFTER_COMMIT_KEY, txn.commitMillis, null);
        } catch(RuntimeException e1) {
            try {
                rollbackInternal(session, txn);
                // Only retryable exception from this store
                if(allowRetry && (e1 instanceof LockTimeoutException)) {
                    clearState = false;
                    shouldRetry = true;
                } else {
                    re = e1;
                }
            } catch(RuntimeException e2) {
                re = e2;
            }
        } finally {
            end(session, txn, clearState, re);
        }
        return shouldRetry;
    }

    private static byte[] copy(byte[] bytes) {
        return (bytes == null) ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static void end(Session session, MemoryTransactionImpl txn, boolean clearState, RuntimeException cause) {
        RuntimeException re = cause;
        try {
            assert session.get(TXN_KEY) == txn;
            if(clearState) {
                session.remove(TXN_KEY);
            }
            txn.runUnlock();
        } catch(RuntimeException e) {
            re = MultipleCauseException.combine(re, e);
        } finally {
            clearStack(session, PRE_COMMIT_KEY);
            clearStack(session, AFTER_COMMIT_KEY);
            clearStack(session, AFTER_ROLLBACK_KEY);
            runCallbacks(session, AFTER_END_KEY, -1, re);
        }
    }

    private static Session.StackKey<Callback> getCallbackKey(CallbackType type) {
        switch(type) {
            case PRE_COMMIT:    return PRE_COMMIT_KEY;
            case COMMIT:        return AFTER_COMMIT_KEY;
            case ROLLBACK:      return AFTER_ROLLBACK_KEY;
            case END:           return AFTER_END_KEY;
        }
        throw new IllegalArgumentException(String.valueOf(type));
    }

    private static MemoryTransactionImpl getTransactionInternal(Session session) {
        MemoryTransactionImpl txn = session.get(TXN_KEY);
        if(txn == null) {
            throw new NoTransactionInProgressException();
        }
        return txn;
    }

    private static void rollbackInternal(Session session, MemoryTransactionImpl txn) {
        txn.runUndo();
        runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
    }

    private static void runCallbacks(Session session, Session.StackKey<Callback> key, long timestamp, RuntimeException cause) {
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
}
