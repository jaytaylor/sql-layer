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
    public static enum CheckTime { 
        IMMEDIATE,
        STATEMENT,
        STATEMENT_WITH_RANGE_CACHE,
        DELAYED,
        DELAYED_WITH_RANGE_CACHE,
        // For testing
        DELAYED_ALWAYS_UNTIL_COMMIT,
        DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT
        ;

        public boolean isDelayed() {
            return (this != IMMEDIATE);
        }

        public boolean isStatement() {
            return (this == STATEMENT) || (this == STATEMENT_WITH_RANGE_CACHE);
        }

        public boolean isTestOnly() {
            return (this == DELAYED_ALWAYS_UNTIL_COMMIT) || (this == DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT);
        }

        public boolean isRanged() {
            return (this == STATEMENT_WITH_RANGE_CACHE) || (this == DELAYED_WITH_RANGE_CACHE) || (this == DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT);
        }

        public CheckTime getNonRanged() {
            switch(this) {
            case STATEMENT_WITH_RANGE_CACHE:
                return STATEMENT;
            case DELAYED_WITH_RANGE_CACHE:
                return DELAYED;
            case DELAYED_WITH_RANGE_CACHE_ALWAYS_UNTIL_COMMIT:
                return DELAYED_ALWAYS_UNTIL_COMMIT;
            default:
                return this;
            }
        }
    }

    static enum CheckPass {
        ROW, STATEMENT, TRANSACTION
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
            if (checkTime.isRanged()) {
                if (!isMonotonic()) 
                    checkTime = checkTime.getNonRanged();
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

        public V getValue(Session session) {
            try {
                return value.get();
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
        }
        public abstract void query(Session session, TransactionState txn, Index index);

        public boolean isDone() {
            return (value != null) && value.isDone();
        }

        public boolean delayOrDefer(CheckTime checkTime, CheckPass pass,
                                    Session session, TransactionState txn, Index index) {
            switch (pass) {
            case ROW:
                return checkTime.isDelayed();
            case STATEMENT:
                return !checkTime.isStatement();
            case TRANSACTION:
            default:
                return false;
            }
        }

        public void blockUntilReady(TransactionState txn) {
            long startNanos = System.nanoTime();
            value.blockUntilReady();
            long endNanos = System.nanoTime();
            txn.uniquenessTime += (endNanos - startNanos);
        }

        public boolean deferForRecheck(CheckPass pass) {
            return false;
        }

        /** Return <code>true</code> if the check passes. */
        public abstract boolean check(Session session, TransactionState txn, Index index);

        /** Create appropriate exception for failed check. */
        public abstract RuntimeException createException(Session session, TransactionState txn, Index index);
    }

    static class KeyDoesNotExistInIndexCheck extends PendingCheck {
        final byte[] ekey;

        public KeyDoesNotExistInIndexCheck(byte[] bkey, byte[] ekey) {
            super(bkey);
            this.ekey = ekey;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void query(Session session, TransactionState txn, Index index) {
            if(ekey == null) {
                value = txn.getFuture(bkey);
            } else {
                value = txn.getRangeAsFutureList(bkey, ekey, 1);
            }
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                return (getValue(session) == null);
            } else {
                return ((List)getValue(session)).isEmpty();
            }
        }

        @Override
        public RuntimeException createException(Session session, TransactionState txn, Index index) {
            // Recover Key for error message.
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String msg = formatIndexRowString(index, persistitKey);
            return new DuplicateKeyException(index.getIndexName(), msg);
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
            value = txn.getSnapshotRangeAsFutureList(bkey, indexEnd, 1, false);
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (false) {
                // This is how you'd find a duplicate from the range. Not used
                // because want to get conflict from individual keys that are
                // checked.
                List<KeyValue> kvs = getValue(session);
                return (kvs.isEmpty() || !Arrays.equals(kvs.get(0).getKey(), bkey));
            }
            else {
                return true;
            }
        }

        @Override
        public RuntimeException createException(Session session, TransactionState txn, Index index) {
            assert false;
            return null;
        }
    }

    static enum DeferredForeignKey {
        IMMEDIATE,
        DEFERRABLE_STATEMENT, DEFERRABLE_TRANSACTION,
        RECHECK_STATEMENT, RECHECK_TRANSACTION
    }

    static abstract class ForeignKeyCheck<V> extends PendingCheck<V> {
        protected final ForeignKey foreignKey;
        protected final String operation;
        protected DeferredForeignKey deferred;

        protected ForeignKeyCheck(byte[] bkey, ForeignKey foreignKey, CheckPass finalPass, String operation) {
            super(bkey);
            this.foreignKey = foreignKey;
            switch (finalPass) {
            case ROW:
                this.deferred = DeferredForeignKey.IMMEDIATE;
                break;
            case STATEMENT:
                this.deferred = DeferredForeignKey.DEFERRABLE_STATEMENT;
                break;
            case TRANSACTION:
                this.deferred = DeferredForeignKey.DEFERRABLE_TRANSACTION;
                break;
            }
            this.operation = operation;
        }

        @Override
        public boolean delayOrDefer(CheckTime checkTime, CheckPass pass,
                                    Session session, TransactionState txn, Index index) {
            switch (deferred) {
            case IMMEDIATE:
            case DEFERRABLE_TRANSACTION:
            default:
                return super.delayOrDefer(checkTime, pass, session, txn, index);
            case DEFERRABLE_STATEMENT:
                // Since we need to recheck any error at the end of the statement, we
                // cannot delay past that, by which time more may have
                // changed. Application should DEFER as well as DELAY to get full
                // benefit when not auto-commit.
                if (pass == CheckPass.STATEMENT)
                    return false;
                else
                    return super.delayOrDefer(checkTime, pass, session, txn, index);
            case RECHECK_STATEMENT:
                if (pass == CheckPass.ROW)
                    return true;
                break;
            case RECHECK_TRANSACTION:
                if (pass != CheckPass.TRANSACTION)
                    return true;
                break;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Repeating check at {}: {}", pass, createException(session, txn, index));
            }
            recheck(session, txn, index); // Execute deferred recheck.
            return false;
        }

        @Override
        public boolean deferForRecheck(CheckPass pass) {
            switch (deferred) {
            case DEFERRABLE_STATEMENT:
                if (pass == CheckPass.ROW) {
                    deferred = DeferredForeignKey.RECHECK_STATEMENT;
                    value = null;
                    return true;
                }
                break;
            case DEFERRABLE_TRANSACTION:
                if (pass != CheckPass.TRANSACTION) {
                    deferred = DeferredForeignKey.RECHECK_TRANSACTION;
                    value = null;
                    return true;
                }
                break;
            }
            return false;
        }

        protected void recheck(Session session, TransactionState txn, Index index) {
            query(session, txn, index);
        }
    }

    static class ForeignKeyReferencingCheck extends ForeignKeyCheck {
        private final byte[] ekey;

        public ForeignKeyReferencingCheck(byte[] bkey, byte[] ekey,
                                          ForeignKey foreignKey, CheckPass finalPass, String operation) {
            super(bkey, foreignKey, finalPass, operation);
            this.ekey = ekey;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void query(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                value = txn.getFuture(bkey);
            } else {
                value = txn.getRangeAsFutureList(bkey, ekey, 1);
            }
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            if (ekey == null) {
                return getValue(session) != null;
            } else {
                return !((List)getValue(session)).isEmpty();
            }
        }

        @Override
        public RuntimeException createException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencingColumns(),
                                                     foreignKey.getReferencedColumns());
            return new ForeignKeyReferencingViolationException(operation,
                                                               foreignKey.getReferencingTable().getName(),
                                                               key,
                                                               foreignKey.getConstraintName().getTableName(),
                                                               foreignKey.getReferencedTable().getName());
        }
    }

    static class ForeignKeyNotReferencedCheck extends ForeignKeyCheck<List<KeyValue>> {
        protected byte[] ekey;
        
        public ForeignKeyNotReferencedCheck(byte[] bkey, byte[] ekey,
                                            ForeignKey foreignKey, CheckPass finalPass, String operation) {
            super(bkey, foreignKey, finalPass, operation);
            this.ekey = ekey;
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            // Only need to find 1, referenced check on insert referencing covers other half
            value = txn.getRangeAsFutureList(bkey, ekey, checkSize());
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            return (getValue(session).size() < checkSize());
        }

        protected int checkSize() {
            return 1;
        }

        @Override
        public RuntimeException createException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            return new ForeignKeyReferencedViolationException(operation,
                                                              foreignKey.getReferencedTable().getName(),
                                                              key,
                                                              foreignKey.getConstraintName().getTableName(),
                                                              foreignKey.getReferencingTable().getName());
        }
    }

    static class ForeignKeyNotReferencedSkipSelfCheck extends ForeignKeyNotReferencedCheck {
        private boolean recheck;

        public ForeignKeyNotReferencedSkipSelfCheck(byte[] bkey, byte[] ekey,
                                                    ForeignKey foreignKey, CheckPass finalPass, String operation) {
            super(bkey, ekey, foreignKey, finalPass, operation);
        }
        
        @Override
        protected int checkSize() {
            return recheck ? 1 : 2; // On recheck, self-referencing row has been deleted.
        }

        @Override
        protected void recheck(Session session, TransactionState txn, Index index) {
            super.recheck(session, txn, index);
            recheck = true;
        }
    }

    static class ForeignKeyNotReferencedWholeCheck extends ForeignKeyCheck<Boolean> {
        protected AsyncIterator<KeyValue> iter;

        public ForeignKeyNotReferencedWholeCheck(byte[] bkey,
                                                 ForeignKey foreignKey, CheckPass finalPass, String operation) {
            super(bkey, foreignKey, finalPass, operation);
        }

        @Override
        public void query(Session session, TransactionState txn, Index index) {
            byte[] indexEnd = ByteArrayUtil.strinc(FDBStoreDataHelper.prefixBytes(index));
            iter = txn.getRangeIterator(bkey, indexEnd);
            value = iter.onHasNext();
        }

        @Override
        public boolean check(Session session, TransactionState txn, Index index) {
            try {
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
            } catch (RuntimeException e) {
                throw FDBAdapter.wrapFDBException(session, e);
            }
            return true;
        }

        @Override
        public RuntimeException createException(Session session, TransactionState txn, Index index) {
            Key persistitKey = new Key((Persistit)null);
            FDBStoreDataHelper.unpackTuple(index, persistitKey, bkey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            return new ForeignKeyReferencedViolationException(operation,
                                                              foreignKey.getReferencedTable().getName(),
                                                              key,
                                                              foreignKey.getConstraintName().getTableName(),
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

    public boolean isDelayed() {
        return checkTime.isDelayed();
    }

    public static PendingCheck<?> keyDoesNotExistInIndexCheck(Session session, TransactionState txn,
                                                              Index index, Key key) {

        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        // The first check of an index in a transaction; may benefit
        // from a range scan to inform the cache of empty space.
        FDBPendingIndexChecks indexChecks = txn.getIndexChecks(false);
        if (indexChecks != null) {
            Map<Index,PendingChecks> pending = indexChecks.pending;
            PendingChecks checks = pending.get(index);
            if (checks == null) {
                checks = new PendingChecks(index);
                pending.put(index, checks);
                CheckTime checkTime = checks.getCheckTime(session, txn,
                                                          indexChecks.checkTime);
                if (checkTime.isRanged()) {
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
                                                             ForeignKey foreignKey, CheckPass finalPass, String operation) {

        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        byte[] ekey = null;
        if (key.getDepth() < index.getAllColumns().size()) {
            ekey = FDBStoreDataHelper.packedTuple(index, key, Key.AFTER);
        }
        PendingCheck<?> check = new ForeignKeyReferencingCheck(bkey, ekey, foreignKey, finalPass, operation);
        check.query(session, txn, index);
        return check;
    }

    public static PendingCheck<?> foreignKeyNotReferencedCheck(Session session, TransactionState txn,
                                                               Index index, Key key, boolean wholeIndex,
                                                               ForeignKey foreignKey, boolean selfReference, CheckPass finalPass, String operation) {
        byte[] bkey = FDBStoreDataHelper.packedTuple(index, key);
        PendingCheck<?> check;
        if (wholeIndex) {
            check = new ForeignKeyNotReferencedWholeCheck(bkey, foreignKey, finalPass, operation);
        }
        else {
            byte[] ekey = FDBStoreDataHelper.packedTuple(index, key, Key.AFTER);
            if (selfReference) {
                check = new ForeignKeyNotReferencedSkipSelfCheck(bkey, ekey, foreignKey, finalPass, operation);
            }
            else {
                check = new ForeignKeyNotReferencedCheck(bkey, ekey, foreignKey, finalPass, operation);
            }
        }
        check.query(session, txn, index);
        return check;
    }

    public void add(Session session, TransactionState txn,
                    Index index, PendingCheck<?> check) {
        // Do this periodically just to keep the size of things down.
        performChecks(session, txn, CheckPass.ROW);
        PendingChecks checks = pending.get(index);
        if (checks == null) {
            checks = new PendingChecks(index);
            pending.put(index, checks);
        }
        checks.add(check);
        metric.increment();
    }
    
    protected void performChecks(Session session, TransactionState txn, CheckPass pass) {
        if (checkTime.isTestOnly() && (pass != CheckPass.TRANSACTION))
            // Special test-only mode to avoid unpredictable timing.
            return;
        int count = 0;
        for (PendingChecks checks : pending.values()) {
            Iterator<PendingCheck<?>> iter = checks.getPending().iterator();
            while (iter.hasNext()) {
                PendingCheck<?> check = iter.next();
                if (!check.isDone()) {
                    if (check.delayOrDefer(checkTime, pass,
                                           session, txn, checks.index)) {
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
                    if (check.deferForRecheck(pass)) {
                        continue;
                    }
                    throw check.createException(session, txn, checks.index);
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
