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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.virtual.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;

import java.util.HashMap;
import java.util.Map;

public class SimpleTableStatusCache implements TableStatusCache {
    private final Map<Integer,VirtualTableStatus> virtualStatusMap = new HashMap<>();
            
    @Override
    public synchronized TableStatus createTableStatus(Table table) {
        return new SimpleTableStatus(table.getTableId());
    }
    
    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new SimpleTableStatus(tableID);
    }

    @Override
    public synchronized TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory) {
        VirtualTableStatus status = virtualStatusMap.get(tableID);
        if(status == null) {
            status = new VirtualTableStatus(tableID, factory);
            virtualStatusMap.put(tableID, status);
        }
        return status;
    }

    @Override
    public void detachAIS() {
        // None
    }

    @Override
    public synchronized void clearTableStatus(Session session, Table table) {
        virtualStatusMap.remove(table.getTableId());
    }

    //
    // Internal
    //

    private static class SimpleTableStatus implements TableStatus
    {
        private final int tableID;
        private long rowCount;

        public SimpleTableStatus(int tableID) {
            this.tableID = tableID;
        }

        @Override
        public synchronized void rowDeleted(Session session) {
            --rowCount;
        }

        @Override
        public synchronized void rowsWritten(Session session, long count) {
            this.rowCount += count;
        }

        @Override
        public synchronized void truncate(Session session) {
            this.rowCount = 0;
        }

        @Override
        public synchronized long getRowCount(Session session) {
            return rowCount;
        }

        @Override
        public synchronized long getApproximateRowCount(Session session) {
            return rowCount;
        }

        @Override
        public int getTableID() {
            return tableID;
        }

        @Override
        public synchronized void setRowCount(Session session, long rowCount) {
            this.rowCount = rowCount;
        }
    }
}
