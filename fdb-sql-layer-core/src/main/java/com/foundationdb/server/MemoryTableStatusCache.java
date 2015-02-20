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

package com.foundationdb.server;

import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryTransaction;
import com.foundationdb.server.store.MemoryTransactionService;
import com.foundationdb.server.store.format.MemoryStorageDescription;

import java.util.HashMap;
import java.util.Map;

import static com.foundationdb.server.store.MemoryStore.join;
import static com.foundationdb.server.store.MemoryStore.packLong;
import static com.foundationdb.server.store.MemoryStore.packUUID;
import static com.foundationdb.server.store.MemoryStore.unpackLong;

public class MemoryTableStatusCache implements TableStatusCache
{
    private final Map<Integer,VirtualTableStatus> virtualTableStatusMap = new HashMap<>();
    private final MemoryTransactionService txnService;
    private final byte[] statusPrefix;

    public MemoryTableStatusCache(MemoryTransactionService txnService, byte[] statusPrefix) {
        this.txnService = txnService;
        this.statusPrefix = statusPrefix;
    }

    public byte[] getStatusPrefix() {
        return statusPrefix;
    }

    //
    // TableStatusCache
    //

    @Override
    public TableStatus createTableStatus(Table table) {
        return new MemoryTableStatus(table);
    }

    @Override
    public synchronized TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory) {
        VirtualTableStatus status = virtualTableStatusMap.get(tableID);
        if(status == null) {
            status = new VirtualTableStatus(tableID, factory);
            virtualTableStatusMap.put(tableID, status);
        }
        return status;
    }

    @Override
    public void detachAIS() {
        // None
    }

    @Override
    public void clearTableStatus(Session session, Table table) {
        TableStatus status = table.tableStatus();
        if(status instanceof VirtualTableStatus) {
            synchronized(virtualTableStatusMap) {
                virtualTableStatusMap.remove(table.getTableId());
            }
        } else {
            MemoryTableStatus tmStatus = (MemoryTableStatus)status;
            MemoryTransaction txn = txnService.getTransaction(session);
            txn.clear(tmStatus.statusKey);
        }
    }


    //
    // Internal
    //

    private class MemoryTableStatus implements TableStatus
    {
        private final int tableID;
        private final byte[] statusKey;

        private MemoryTableStatus(Table table) {
            this.tableID = table.getTableId();
            // packLong(tableID) seems like a good option but ALTER keeps same ID => doubles row count
            PrimaryKey pk = table.getPrimaryKeyIncludingInternal();
            assert pk != null : table;
            MemoryStorageDescription sd = (MemoryStorageDescription)pk.getIndex().getStorageDescription();
            this.statusKey = join(statusPrefix, sd.getUUIDBytes());
        }

        @Override
        public void rowDeleted(Session session) {
            rowsWritten(session, -1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            setRowCount(session, getRowCount(session) + count);
        }

        @Override
        public void truncate(Session session) {
            setRowCount(session, 0);
        }

        @Override
        public long getRowCount(Session session) {
            MemoryTransaction txn = txnService.getTransaction(session);
            byte[] value = txn.get(statusKey);
            return (value == null) ? 0 : unpackLong(value);
        }

        @Override
        public long getApproximateRowCount(Session session) {
            MemoryTransaction txn = txnService.getTransaction(session);
            byte[] value = txn.getUncommitted(statusKey);
            return (value == null) ? 0 : unpackLong(value);
        }

        @Override
        public int getTableID() {
            return tableID;
        }

        @Override
        public void setRowCount(Session session, long rowCount) {
            MemoryTransaction txn = txnService.getTransaction(session);
            txn.set(statusKey, packLong(rowCount));
        }
    }
}
