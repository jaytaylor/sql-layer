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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.session.Session;

import com.foundationdb.KeyValue;
import com.foundationdb.ReadTransaction;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.ByteArrayUtil;
import com.persistit.Key;
import com.persistit.Persistit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FDBPendingIndexChecks
{
    static enum CheckTime { IMMEDIATE, DEFERRED, DEFERRED_WITH_RANGE_CACHE };

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
                Type type = columns.get(0).getColumn().getType();
                if ((type == Types.INT) || (type == Types.BIGINT)) {
                    return true;
                }
            }
            return false;
        }
    }

    static abstract class PendingCheck<V> {
        protected final byte[] bkey;
        protected Future<V> value;

        protected PendingCheck(byte[] bkey) {
            this.bkey = bkey;
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
        public abstract boolean check();

        /** Throw appropriate exception for failed check. */
        public abstract void throwException(Session session, TransactionState txn, Index index);
    }

    static class KeyDoesNotExistInIndexCheck extends PendingCheck<byte[]> {
        public KeyDoesNotExistInIndexCheck(byte[] bkey) {
            super(bkey);
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            value = txn.getTransaction().get(bkey);
        }

        @Override
        public boolean check() {
            return (value.get() == null);
        }

        @Override
        public void throwException(Session session, TransactionState txn, Index index) {
            // Recover Key for error message.
            Key key = new Key((Persistit)null);
            FDBStore.unpackTuple(index, key, bkey);
            String msg = formatIndexRowString(index, key);
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
            byte[] indexEnd = ByteArrayUtil.strinc(FDBStore.prefixBytes(index));
            value = txn.getTransaction().snapshot().getRange(bkey, indexEnd, 1).asList();
        }

        @Override
        public boolean check() {
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

        byte[] bkey = FDBStore.packedTuple(index, key);
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
        PendingCheck<?> check = new KeyDoesNotExistInIndexCheck(bkey);
        check.query(session, txn, index);
        return check;
    }

    public void add(Session session, TransactionState txn,
                    Index index, PendingCheck<?> check) {
        // Do this periodically just to keep the size of things down.
        performChecks(session, txn, false);
        pending.get(index).add(check);
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
                if (!check.check()) {
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
