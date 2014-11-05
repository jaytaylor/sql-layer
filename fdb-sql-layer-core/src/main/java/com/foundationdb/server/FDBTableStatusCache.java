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

package com.foundationdb.server;

import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.Database;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   tableStatus/
 * </pre>
 *
 * <p>
 *     The above directory is used to store auto-inc, unique and/or row count
 *     information on a per-table basis. Each key is formed by pre-pending the
 *     directory prefix with the primary key's prefix and  the string
 *     "autoInc", "unique" or "rowCount". The auto-inc and unique values are
 *     {@link Tuple} encoded longs and the row count is a little-endian encoded
 *     long (for {@link Transaction#mutate} usage).
 * </p>
 */
public class FDBTableStatusCache implements TableStatusCache {
    private static final List<String> TABLE_STATUS_DIR_PATH = Arrays.asList("tableStatus");
    private static final byte[] AUTO_INC_PACKED = Tuple2.from("autoInc").pack();
    private static final byte[] UNIQUE_PACKED = Tuple2.from("unique").pack();
    private static final byte[] ROW_COUNT_PACKED = Tuple2.from("rowCount").pack();


    private final Database db;
    private final FDBTransactionService txnService;
    private final Map<Integer,MemoryTableStatus> memoryTableStatusMap = new HashMap<>();

    private byte[] packedTableStatusPrefix;


    public FDBTableStatusCache(FDBHolder holder, FDBTransactionService txnService) {
        this.db = holder.getDatabase();
        this.txnService = txnService;
        this.packedTableStatusPrefix = holder.getRootDirectory().createOrOpen(holder.getDatabase(),
                                                                              TABLE_STATUS_DIR_PATH).get().pack();
    }


    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new FDBTableStatus(tableID);
    }

    @Override
    public synchronized TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryTableStatus status = memoryTableStatusMap.get(tableID);
        if(status == null) {
            status = new MemoryTableStatus(tableID, factory);
            memoryTableStatusMap.put(tableID, status);
        }
        return status;
    }

    @Override
    public synchronized void detachAIS() {
        for(MemoryTableStatus status : memoryTableStatusMap.values()) {
            status.setRowDef(null);
        }
    }

    @Override
    public synchronized void clearTableStatus(Session session, Table table) {
        TableStatus status = table.rowDef().getTableStatus();
        if(status instanceof FDBTableStatus) {
            ((FDBTableStatus)status).clearState(session);
        } else if(status != null) {
            assert status instanceof MemoryTableStatus : status;
            memoryTableStatusMap.remove(table.getTableId());
        }
    }

    public static byte[] packForAtomicOp(long l) {
        byte[] bytes = new byte[8];
        AkServerUtil.putLong(bytes, 0, l);
        return bytes;
    }

    public static long unpackForAtomicOp(byte[] bytes) {
        if(bytes == null) {
            return 0;
        }
        assert bytes.length == 8 : bytes.length;
        return AkServerUtil.getLong(bytes, 0);
    }


    /**
     * Use FDBCounter for row count and single k/v for others.
     */
    private class FDBTableStatus implements TableStatus {
        private final int tableID;
        private volatile byte[] rowCountKey;
        private volatile byte[] autoIncKey;

        public FDBTableStatus(int tableID) {
            this.tableID = tableID;

        }

        @Override
        public void rowDeleted(Session session) {
            rowsWritten(session, -1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            TransactionState txn = txnService.getTransaction(session);
            txn.mutate(MutationType.ADD, rowCountKey, packForAtomicOp(count));
        }

        @Override
        public void truncate(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            txn.setBytes(rowCountKey, packForAtomicOp(0));
            internalSetAutoInc(session, 0, true);
        }

        @Override
        public void setAutoIncrement(Session session, long value) {
            internalSetAutoInc(session, value, false);
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            if(rowDef == null) {
                this.autoIncKey = null;
                this.rowCountKey = null;
            } else {
                assert rowDef.getRowDefId() == tableID;
                byte[] prefixBytes = FDBStoreDataHelper.prefixBytes(rowDef.getPKIndex());
                this.autoIncKey = ByteArrayUtil.join(packedTableStatusPrefix, prefixBytes, AUTO_INC_PACKED);
                this.rowCountKey = ByteArrayUtil.join(packedTableStatusPrefix, prefixBytes, ROW_COUNT_PACKED);
            }
        }

        @Override 
        public void setIndex(TableIndex pkTableIndex) {
            if (pkTableIndex == null) {
                this.autoIncKey = null;
                this.rowCountKey = null;
            } else {
                assert pkTableIndex.getTable().getTableId() == tableID;
                byte[] prefixBytes = FDBStoreDataHelper.prefixBytes(pkTableIndex);
                this.autoIncKey = ByteArrayUtil.join(packedTableStatusPrefix, prefixBytes, AUTO_INC_PACKED);
                this.rowCountKey = ByteArrayUtil.join(packedTableStatusPrefix, prefixBytes, ROW_COUNT_PACKED);
                
            }
        }

        @Override
        public long getAutoIncrement(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            return decodeOrZero(txn.getValue(autoIncKey));
        }

        @Override
        public long getRowCount(Session session) {
            return getRowCount (txnService.getTransaction(session), false);
        }

        @Override
        public long getApproximateRowCount(Session session) {
            // TODO: Snapshot avoids conflicts but might still round trip. Cache locally for some time frame?
            return getRowCount(txnService.getTransaction(session), true);
        }

        @Override
        public int getTableID() {
            return tableID;
        }

        @Override
        public void setRowCount(Session session, long rowCount) {
            TransactionState txn = txnService.getTransaction(session);
            txn.setBytes(rowCountKey, packForAtomicOp(rowCount));
        }

        private void clearState(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            txn.clearKey(rowCountKey);
            txn.clearKey(autoIncKey);
        }

        private void internalSetAutoInc(Session session, long value, boolean evenIfLess) {
            TransactionState txn = txnService.getTransaction(session);
            long current = decodeOrZero(txn.getValue(autoIncKey));
            if(evenIfLess || value > current) {
                txn.setBytes(autoIncKey, Tuple2.from(value).pack());
            }
        }

        private long decodeOrZero(byte[] bytes) {
            return (bytes == null) ? 0 : Tuple2.fromBytes(bytes).getLong(0);
        }

        private long getRowCount(TransactionState txn, boolean snapshot) {
            if (snapshot) {
                return unpackForAtomicOp(txn.getSnapshotValue(rowCountKey));
            } else {
                return unpackForAtomicOp(txn.getValue(rowCountKey));
            }
        }
    }
}
