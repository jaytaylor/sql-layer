/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.FDBTransactionService;
import com.akiban.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.Database;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class FDBTableStatusCache implements TableStatusCache {
    private final Database db;
    private final FDBTransactionService txnService;
    private final Map<Integer,MemoryTableStatus> memoryTableStatusMap = new HashMap<>();


    public FDBTableStatusCache(Database db, FDBTransactionService txnService) {
        this.db = db;
        this.txnService = txnService;
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
    public synchronized void clearTableStatus(Session session, UserTable table) {
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
        private volatile byte[] uniqueKey;

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
            txn.getTransaction().mutate(MutationType.ADD, rowCountKey, packForAtomicOp(count));
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
                this.uniqueKey = null;
                this.rowCountKey = null;
            } else {
                assert rowDef.getRowDefId() == tableID;
                String treeName = rowDef.getPKIndex().indexDef().getTreeName();
                this.autoIncKey = Tuple.from("tableStatus", treeName, "autoInc").pack();
                this.uniqueKey = Tuple.from("tableStatus", treeName, "unique").pack();
                this.rowCountKey = Tuple.from("tableStatus", treeName, "rowCount").pack();
            }
        }

        @Override
        public long createNewUniqueID(Session session) {
            // Use new transaction to avoid conflicts. Can result in gaps, but that's OK.
            final long[] newValue = { 0 };
            try {
                db.run(new Function<Transaction,Void>() {
                    @Override
                    public Void apply(Transaction txn) {
                        byte[] curBytes = txn.get(uniqueKey).get();
                        newValue[0] = 1 + decodeOrZero(curBytes);
                        txn.set(uniqueKey, Tuple.from(newValue[0]).pack());
                        return null;
                    }
                });
            } catch(Exception e) {
                throw new AkibanInternalException("", e);
            } catch(Throwable t) {
                Error e = new Error();
                e.initCause(t);
                throw e;
            }
            return newValue[0];
        }

        @Override
        public long getAutoIncrement(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            byte[] bytes = txn.getTransaction().get(autoIncKey).get();
            return decodeOrZero(bytes);
        }

        @Override
        public long getRowCount(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            return unpackForAtomicOp(txn.getTransaction().get(rowCountKey).get());
        }

        @Override
        public long getApproximateRowCount() {
            // TODO: Avoids conflicts but still round trip. Cache locally for some time frame?
            Transaction txn = db.createTransaction();
            return unpackForAtomicOp(txn.get(rowCountKey).get());
        }

        @Override
        public long getUniqueID(Session session) {
            // Use new transaction to avoid conflicts.
            final long[] currentValue = { 0 };
            try {
                db.run(new Function<Transaction,Void>() {
                    @Override
                    public Void apply(Transaction txn) {
                        byte[] curBytes = txn.snapshot().get(uniqueKey).get();
                        currentValue[0] = decodeOrZero(curBytes);
                        return null;
                    }
                });
            } catch(Exception e) {
                throw new AkibanInternalException("", e);
            } catch(Throwable t) {
                Error e = new Error();
                e.initCause(t);
                throw e;
            }
            return currentValue[0];
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

        @Override
        public long getApproximateUniqueID() {
            // TODO: Avoids conflicts but still round trip. Cache locally for some time frame?
            Transaction txn = db.createTransaction();
            byte[] bytes = txn.get(uniqueKey).get();
            return decodeOrZero(bytes);
        }

        private void clearState(Session session) {
            TransactionState txn = txnService.getTransaction(session);
            txn.getTransaction().clear(rowCountKey);
            txn.getTransaction().clear(autoIncKey);
            txn.getTransaction().clear(uniqueKey);
        }

        private void internalSetAutoInc(Session session, long value, boolean evenIfLess) {
            TransactionState txn = txnService.getTransaction(session);
            long current = decodeOrZero(txn.getTransaction().get(autoIncKey).get());
            if(evenIfLess || value > current) {
                txn.setBytes(autoIncKey, Tuple.from(value).pack());
            }
        }

        private long decodeOrZero(byte[] bytes) {
            return (bytes == null) ? 0 : Tuple.fromBytes(bytes).getLong(0);
        }
    }
}
