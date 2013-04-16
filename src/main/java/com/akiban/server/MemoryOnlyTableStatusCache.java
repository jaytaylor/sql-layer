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
import com.akiban.server.rowdata.RowDef;

import java.util.HashMap;
import java.util.Map;

public class MemoryOnlyTableStatusCache implements TableStatusCache {
    private final Map<Integer, InternalTableStatus> tableStatusMap = new HashMap<>();
            
    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new InternalTableStatus(tableID, null);
    }

    @Override
    public TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        return getInternalTableStatus(tableID, factory);
    }

    @Override
    public synchronized void detachAIS() {
        for(InternalTableStatus status : tableStatusMap.values()) {
            status.setRowDef(null);
        }
    }

    private InternalTableStatus getInternalTableStatus(int tableID, MemoryTableFactory factory) {
        InternalTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            ts = new InternalTableStatus(tableID, factory);
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }

    private static class InternalTableStatus implements TableStatus {
        private final int expectedID;
        private final MemoryTableFactory factory;
        private long autoIncrement = 0;
        private int ordinal = 0;
        private long rowCount = 0;
        private long uniqueID = 0;

        public InternalTableStatus(int expectedID, MemoryTableFactory factory) {
            this.expectedID = expectedID;
            this.factory = factory;
        }

        @Override
        public synchronized long getAutoIncrement() {
            return autoIncrement;
        }

        @Override
        public synchronized int getOrdinal() {
            return ordinal;
        }

        @Override
        public synchronized long getRowCount() {
            if(factory != null) {
                return factory.rowCount();
            }
            return rowCount;
        }

        @Override
        public synchronized void setRowCount(long rowCount) {
            if(factory != null) {
                throw new IllegalArgumentException("Cannot set row count for memory table");
            }
            this.rowCount = rowCount;
        }

        @Override
        public synchronized long getApproximateRowCount() {
            return getRowCount();
        }

        @Override
        public synchronized long getUniqueID() {
            return uniqueID;
        }

        @Override
        public long getApproximateUniqueID() {
            return getUniqueID();
        }

        @Override
        public int getTableID() {
            return expectedID;
        }

        @Override
        public synchronized void setRowDef(RowDef rowDef) {
            if((rowDef != null) && (expectedID != rowDef.getRowDefId())) {
                throw new IllegalArgumentException("RowDef ID " + rowDef.getRowDefId() +
                                                   " does not match expected ID " + expectedID);
            }
        }

        @Override
        public synchronized void rowDeleted() {
            rowCount = Math.max(0, rowCount - 1);
        }

        @Override
        public synchronized void rowsWritten(long count) {
            rowCount += count;
        }

        @Override
        public synchronized void setAutoIncrement(long autoIncrement) {
            this.autoIncrement = Math.max(this.autoIncrement, autoIncrement);
        }

        @Override
        public synchronized void setOrdinal(int ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public synchronized long createNewUniqueID() {
            return ++uniqueID;
        }

        @Override
        public synchronized void truncate() {
            autoIncrement = 0;
            uniqueID = 0;
            rowCount = 0;
        }
    }
}
