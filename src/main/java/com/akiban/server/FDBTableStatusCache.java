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

import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.FDBTransactionService;
import com.akiban.util.FDBCounter;
import com.foundationdb.Database;
import com.foundationdb.Retryable;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class FDBTableStatusCache implements TableStatusCache {
    private final Database db;
    private final FDBTransactionService txnService;
    private final Map<Integer,FDBTableStatus> tableStatusMap = new HashMap<>();
    private final Map<Integer,MemoryTableStatus> memoryTableStatusMap = new HashMap<>();


    public FDBTableStatusCache(Database db, FDBTransactionService txnService) {
        this.db = db;
        this.txnService = txnService;
    }


    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        FDBTableStatus status = tableStatusMap.get(tableID);
        if(status == null) {
            status = new FDBTableStatus(tableID);
            tableStatusMap.put(tableID, status);
            assert !memoryTableStatusMap.containsKey(tableID) : tableID;
        }
        return status;
    }

    @Override
    public synchronized TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryTableStatus status = memoryTableStatusMap.get(tableID);
        if(status == null) {
            status = new MemoryTableStatus(tableID, factory);
            memoryTableStatusMap.put(tableID, status);
            assert !tableStatusMap.containsKey(tableID) : tableID;
        }
        return status;
    }

    @Override
    public synchronized void detachAIS() {
        for(FDBTableStatus status : tableStatusMap.values()) {
            status.setRowDef(null);
        }
    }

    public synchronized void deleteTableStatus(Session session, int tableID) {
        FDBTableStatus status = tableStatusMap.remove(tableID);
        if(status != null) {
            status.clearState(session);
        }
        memoryTableStatusMap.remove(tableID);
    }


    /**
     * Use FDBCounter for row count and single k/v for others.
     */
    private class FDBTableStatus implements TableStatus {
        private final int tableID;
        private final FDBCounter rowCounter;
        private final byte[] autoIncKey;
        private final byte[] uniqueKey;
        private RowDef rowDef;

        public FDBTableStatus(int tableID) {
            this.tableID = tableID;
            // TODO: Request prefix from Store? Delay until rowDef is set and use treeName?
            String prefix = "tableStatus_" + tableID ;
            this.autoIncKey = Tuple.from(prefix + "_autoInc").pack();
            this.uniqueKey = Tuple.from(prefix + "_unique").pack();

            byte[] counterKey = (prefix + "_rowCount").getBytes(Charset.forName("UTF8"));
            this.rowCounter = new FDBCounter(db, counterKey, 0);
        }

        @Override
        public void rowDeleted(Session session) {
            rowsWritten(session, -1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            Transaction txn = txnService.getTransaction(session);
            rowCounter.add(txn, 1);
        }

        @Override
        public void truncate(Session session) {
            Transaction txn = txnService.getTransaction(session);
            rowCounter.set(txn, 0);
            internalSetAutoInc(session, 0, true);
        }

        @Override
        public void setAutoIncrement(Session session, long value) {
            internalSetAutoInc(session, value, false);
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            this.rowDef = rowDef;
        }

        @Override
        public long createNewUniqueID(Session session) {
            // Use new transaction to avoid conflicts. Can result in gaps, but that's OK.
            final long[] newValue = { 0 };
            try {
                db.run(new Retryable() {
                    @Override
                    public void attempt(Transaction txn) {
                        byte[] curBytes = txn.get(uniqueKey).get();
                        newValue[0] = 1 + decodeOrZero(curBytes);
                        txn.set(uniqueKey, Tuple.from(newValue[0]).pack());
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
            Transaction txn = txnService.getTransaction(session);
            byte[] bytes = txn.get(autoIncKey).get();
            return decodeOrZero(bytes);
        }

        @Override
        public long getRowCount(Session session) {
            Transaction txn = txnService.getTransaction(session);
            return rowCounter.getTransactional(txn);
        }

        @Override
        public long getApproximateRowCount() {
            // TODO: Avoids conflicts but still round tipe. Cache locally for some time frame?
            Transaction txn = db.createTransaction();
            return rowCounter.getSnapshot(txn);
        }

        @Override
        public long getUniqueID(Session session) {
            // Use new transaction to avoid conflicts.
            final long[] currentValue = { 0 };
            try {
                db.run(new Retryable() {
                    @Override
                    public void attempt(Transaction txn) {
                        byte[] curBytes = txn.snapshot.get(uniqueKey).get();
                        currentValue[0] = decodeOrZero(curBytes);
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
            Transaction txn = txnService.getTransaction(session);
            rowCounter.set(txn, rowCount);
        }

        @Override
        public long getApproximateUniqueID() {
            // TODO: Avoids conflicts but still round tipe. Cache locally for some time frame?
            Transaction txn = db.createTransaction();
            byte[] bytes = txn.get(uniqueKey).get();
            return (bytes == null) ? 0 : Tuple.fromBytes(bytes).getLong(0);
        }

        private void clearState(Session session) {
            Transaction txn = txnService.getTransaction(session);
            rowCounter.clearState(txn);
            txn.clear(autoIncKey);
            txn.clear(uniqueKey);
        }

        private void internalSetAutoInc(Session session, long value, boolean evenIfLess) {
            Transaction txn = txnService.getTransaction(session);
            long current = decodeOrZero(txn.get(autoIncKey).get());
            if(evenIfLess || value > current) {
                txn.set(autoIncKey, Tuple.from(value).pack());
            }
        }

        private long decodeOrZero(byte[] bytes) {
            return (bytes == null) ? 0 : Tuple.fromBytes(bytes).getLong(0);
        }
    }
}
