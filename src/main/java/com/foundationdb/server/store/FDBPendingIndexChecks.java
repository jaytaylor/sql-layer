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

import com.foundationdb.server.store.FDBTransactionService.TransactionState;

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.ForeignKeyReferencedViolationException;
import com.foundationdb.server.error.ForeignKeyReferencingViolationException;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.session.Session;

import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.persistit.Key;
import com.persistit.Persistit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FDBPendingIndexChecks
{
    static enum CheckTime { 
        IMMEDIATE, 
        DEFERRED, DEFERRED_WITH_RANGE_CACHE,
        DEFERRED_ALWAYS_UNTIL_COMMIT // For testing
    }

    static class PendingChecks {
        protected final Index index;
        protected List<PendingCheck<?>> pending = new ArrayList<>();

        public PendingChecks(Index index) {
            this.index = index;
        }

        public Iterable<PendingCheck<?>> getPending() {
            return pending;
        }

        public void add(PendingCheck<?> check) {
            pending.add(check);
        }

        public int size() {
            return pending.size();
        }

        public void clear() {
            pending.clear();
        }

        public CheckTime getCheckTime(Session session, TransactionState txn,
                                      CheckTime checkTime) {
            if (checkTime == CheckTime.DEFERRED_WITH_RANGE_CACHE) {
                if (!isMonotonic()) 
                    checkTime = CheckTime.DEFERRED;
            }
            return checkTime;
        }

        /** Is the primary key for this index likely to increase monotonically? */
        protected boolean isMonotonic() {
            List<IndexColumn> columns = index.getKeyColumns();
            if (columns.size() == 1) {
                int type = columns.get(0).getColumn().getType().typeClass().jdbcType();
                if ((type == Types.INTEGER) || (type == Types.BIGINT)) {
                    return true;
                }
            }
            return false;
        }
    }

    static abstract class PendingCheck<V> {
        protected byte[] bkey;
        protected Future<V> value;

        protected PendingCheck(byte[] bkey) {
            this.bkey = bkey;
        }

        public byte[] getRawKey() {
            return bkey;
        }

        public abstract void query(Session session, TransactionState txn, Index index);

        public boolean isDone() {
            return value.isDone();
        }

        public void blockUntilReady(TransactionState txn) {
            long startNanos = System.nanoTime();
            value.blockUntilReady();
            long endNanos = System.nanoTime();
            txn.uniquenessTime += (endNanos - startNanos);
        }

        /** Return <code>true</code> if the check passes. */
        public abstract boolean check(Session session, TransactionState txn, Index index);

        /** Throw appropriate exception for failed check. */
        public abstract void throwException(Session session, TransactionState txn, Index index);
    }

    static class KeyDoesNotExistInIndexCheck extends PendingCheck {
        final byte[] ekey;

        public KeyDoesNotExistInIndexCheck(byte[] bkey, byte[] ekey) {
            super(bkey);
            this.ekey = ekey;
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            if(ekey == null) {
                value = txn.getTransaction().get(bkey);
            } else {
                value = txn.getTransaction().getRange(bkey, ekey).asList();
            }
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                return (value.get() == null);
            } else {
                return ((List)value.get()).isEmpty();
            }
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            // Recover Key for error message.
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String msg = formatIndexRowString(index, persistitKey);
            throw new DuplicateKeyException(index.getIndexName(), msg);
        }
    }

    /** This is not a real check, but might be put in the pending queue so that
     * it doesn't get GC'ed too early and cancel the range load into the cache.
     */
    static class SnapshotRangeLoadCache extends PendingCheck<List<KeyValue>> {
        public SnapshotRangeLoadCache(byte[] key) {
            super(key);
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            byte[] indexEnd = ByteArrayUtil.strinc(FDBStoreDataHelper.prefixBytes(index));
            value = txn.getTransaction().snapshot().getRange(bkey, indexEnd, 1).asList();
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (false) {
                // This is how you'd find a duplicate from the range. Not used
                // because want to get conflict from individual keys that are
                // checked.
                List<KeyValue> kvs = value.get();
                return (kvs.isEmpty() || !Arrays.equals(kvs.get(0).getKey(), bkey));
            }
            else {
                return true;
            }
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            assert false;
        }
    }

    static abstract class ForeignKeyCheck<V> extends PendingCheck<V> {
        protected final ForeignKey foreignKey;
        protected final String action;

        protected ForeignKeyCheck(byte[] bkey, ForeignKey foreignKey, String action) {
            super(bkey);
            this.foreignKey = foreignKey;
            this.action = action;
        }
    }

    static class ForeignKeyReferencingCheck extends ForeignKeyCheck {
        private final byte[] ekey;

        public ForeignKeyReferencingCheck(byte[] bkey, byte[] ekey,
                                          ForeignKey foreignKey, String action) {
            super(bkey, foreignKey, action);
            this.ekey = ekey;
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                value = txn.getTransaction().get(bkey);
            } else {
                value = txn.getTransaction().getRange(bkey, ekey).asList();
            }
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                return value.get() != null;
            } else {
                return !((List)value.get()).isEmpty();
            }
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencingColumns(),
                                                     foreignKey.getReferencedColumns());
            throw new ForeignKeyReferencingViolationException(action,
                                                              foreignKey.getReferencingTable().getName(),
                                                              key,
                                                              foreignKey.getConstraintName(),
                                                              foreignKey.getReferencedTable().getName());
        }
    }

    static class ForeignKeyNotReferencedCheck extends ForeignKeyCheck<List<KeyValue>> {
        protected byte[] ekey;
        
        public ForeignKeyNotReferencedCheck(byte[] bkey, byte[] ekey,
                                            ForeignKey foreignKey, String action) {
            super(bkey, foreignKey, action);
            this.ekey = ekey;
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            // Only need to find 1, referenced check on insert referencing covers other half
            value = txn.getTransaction().snapshot().getRange(bkey, ekey, 1).asList();
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            return value.get().isEmpty();
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            throw new ForeignKeyReferencedViolationException(action,
                                                             foreignKey.getReferencedTable().getName(),
                                                             key,
                                                             foreignKey.getConstraintName(),
                                                             foreignKey.getReferencingTable().getName());
        }
    }

    static class ForeignKeyNotReferencedWholeCheck extends ForeignKeyCheck<Boolean> {
        protected AsyncIterator<KeyValue> iter;

        public ForeignKeyNotReferencedWholeCheck(byte[] bkey,
                                                 ForeignKey foreignKey, String action) {
            super(bkey, foreignKey, action);
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            byte[] indexEnd = ByteArrayUtil.strinc(FDBStoreDataHelper.prefixBytes(index));
            iter = txn.getTransaction().snapshot().getRange(bkey, indexEnd).iterator();
            value = iter.onHasNext();
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            Key persistitKey = null;
            while (iter.hasNext()) {
                KeyValue kv = iter.next();
                bkey = kv.getKey();
                if (persistitKey == null) {
                    persistitKey = new Key((Persistit)null);
                }
                FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
                if (!ConstraintHandler.keyHasNullSegments(persistitKey, index)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            throw new ForeignKeyReferencedViolationException(action,
                                                             foreignKey.getReferencedTable().getName(),
                                                             key,
                                                             foreignKey.getConstraintName(),
                                                             foreignKey.getReferencingTable().getName());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(FDBPendingIndexChecks.class);
    private final Map<Index,PendingChecks> pending = new HashMap<>();
    private final CheckTime checkTime;
    private final LongMetric metric;

    public FDBPendingIndexChecks(CheckTime checkTime, LongMetric metric) {
        this.checkTime = checkTime;
        this.metric = metric;
    }

    public static PendingCheck<?> keyDoesNotExistInIndexCheck(Session session, TransactionState txn,
                                                              Index index, Key key) {

        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        // The first check of an index in a transaction; may benefit
        // from a range scan to inform the cache of empty space.
        FDBPendingIndexChecks indexChecks = txn.getIndexChecks();
        if (indexChecks != null) {
            Map<Index,PendingChecks> pending = indexChecks.pending;
            PendingChecks checks = pending.get(index);
            if (checks == null) {
                checks = new PendingChecks(index);
                pending.put(index, checks);
                CheckTime checkTime = checks.getCheckTime(session, txn,
                                                          indexChecks.checkTime);
                if (checkTime == CheckTime.DEFERRED_WITH_RANGE_CACHE) {
                    LOG.debug("One-time range load for {} > {}", index, key);
                    PendingCheck<?> check = new SnapshotRangeLoadCache(bkey);
                    check.query(session, txn, index);
                    // TODO: Queuing seems to never help, so always block for now.
                    if (true) {
                        check.blockUntilReady(txn);
                    }
                    else {
                        indexChecks.add(session, txn, index, check);
                    }
                }
            }
        }
        byte[] ekey = null;
        // Check entire range of prefix for key with unspecified components (i.e. HKey columns)
        if (key.getDepth() < index.getAllColumns().size()) {
            ekey = FDBStoreDataHelper.packedTuple(index, key, Key.AFTER);
        }
        PendingCheck<?> check = new KeyDoesNotExistInIndexCheck(bkey, ekey);
        check.query(session, txn, index);
        return check;
    }

    public static PendingCheck<?> foreignKeyReferencingCheck(Session session, TransactionState txn,
                                                             Index index, Key key,
                                                             ForeignKey foreignKey, String action) {

        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        byte[] ekey = null;
        if (key.getDepth() < index.getAllColumns().size()) {
            ekey = FDBStoreDataHelper.packedTuple(index, key, Key.AFTER);
        }
        PendingCheck<?> check = new ForeignKeyReferencingCheck(bkey, ekey, foreignKey, action);
        check.query(session, txn, index);
        return check;
    }

    public static PendingCheck<?> foreignKeyNotReferencedCheck(Session session, TransactionState txn,
                                                               Index index, Key key, boolean wholeIndex,
                                                               ForeignKey foreignKey, String action) {
        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        PendingCheck<?> check;
        if (wholeIndex) {
            check = new ForeignKeyNotReferencedWholeCheck(bkey, foreignKey, action);
        }
        else {
            byte[] ekey = FDBStoreDataHelper.packedTuple(index, key, Key.AFTER);
            check = new ForeignKeyNotReferencedCheck(bkey, ekey, foreignKey, action);
        }
        check.query(session, txn, index);
        return check;
    }

    public void add(Session session, TransactionState txn,
                    Index index, PendingCheck<?> check) {
        // Do this periodically just to keep the size of things down.
        if (checkTime != CheckTime.DEFERRED_ALWAYS_UNTIL_COMMIT) {
            performChecks(session, txn, false);
        }
        PendingChecks checks = pending.get(index);
        if (checks == null) {
            checks = new PendingChecks(index);
            pending.put(index, checks);
        }
        checks.add(check);
        metric.increment();
    }
    
    protected void performChecks(Session session, TransactionState txn, boolean wait) {
        int count = 0;
        for (PendingChecks checks : pending.values()) {
            Iterator<PendingCheck<?>> iter = checks.getPending().iterator();
            while (iter.hasNext()) {
                PendingCheck<?> check = iter.next();
                if (!check.isDone()) {
                    if (!wait) {
                        continue;
                    }
                    if (count > 0) {
                        // Don't bother updating count for every one
                        // done, but do before actually blocking.
                        metric.increment(- count);
                        count = 0;
                    }
                    check.blockUntilReady(txn);
                }
                boolean ok;
                try {
                    ok = check.check(session, txn, checks.index);
                }
                catch (Exception ex) {
                    throw FDBAdapter.wrapFDBException(session, ex);
                }
                if (!ok) {
                    check.throwException(session, txn, checks.index);
                    assert false : check + " did not throw an exception";
                }
                iter.remove();
                count++;
            }
        }
        if (count > 0) {
            metric.increment(- count);
        }
    }

    public void clear() {
        int count = 0;
        for (PendingChecks checks : pending.values()) {
            count += checks.size();
            checks.clear();
        }
        if (count > 0) {
            metric.increment(- count);
        }
    }

    private static String formatIndexRowString(Index index, Key key) {
        StringBuilder sb = new StringBuilder();
        int maxDecode = index.getKeyColumns().size();
        sb.append('(');
        key.indexTo(0);
        for(int i = 0; i < maxDecode && i < key.getDepth(); ++i) {
            if(i > 0) {
                sb.append(',');
            }
            Object o = key.decode();
            sb.append(o);
        }
        sb.append(')');
        return sb.toString();
    }
}
