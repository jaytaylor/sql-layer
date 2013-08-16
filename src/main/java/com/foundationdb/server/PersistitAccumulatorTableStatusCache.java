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

import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.qp.persistitadapter.PersistitAdapter;
import com.foundationdb.server.error.PersistitAdapterException;
import com.foundationdb.server.rowdata.IndexDef;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.TreeService;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.HashMap;
import java.util.Map;

public class PersistitAccumulatorTableStatusCache implements TableStatusCache {
    private Map<Integer, MemoryStatus> memoryStatuses = new HashMap<>();
    private TreeService treeService;

    public PersistitAccumulatorTableStatusCache(TreeService treeService) {
        this.treeService = treeService;
    }

    public synchronized void clearTableStatus(Session session, UserTable table) {
        // Nothing for the status itself, Accumulators attached to Tree
        memoryStatuses.remove(table.getTableId());
    }

    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new AccumulatorStatus(tableID);
    }

    @Override
    public TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryStatus ts = memoryStatuses.get(tableID);
        if(ts == null) {
            if(factory == null) {
                throw new IllegalArgumentException("Null factory");
            }
            ts = new MemoryStatus(tableID, factory);
            memoryStatuses.put(tableID, ts);
        }
        return ts;
    }

    @Override
    public synchronized void detachAIS() {
        for(MemoryStatus ts : memoryStatuses.values()) {
            ts.setRowDef(null);
        }
    }

    public int recoverAccumulatorOrdinal(TableStatus tableStatus) {
        if(!(tableStatus instanceof AccumulatorStatus)) {
            throw new IllegalArgumentException("Expected AccumulatorStatus: " + tableStatus);
        }
        return ((AccumulatorStatus)tableStatus).getOrdinal();
    }

    //
    // Internal
    //

    private static void checkExpectedRowDefID(int expected, RowDef rowDef) {
        if((rowDef != null) && (expected != rowDef.getRowDefId())) {
            throw new IllegalArgumentException("RowDef ID " + rowDef.getRowDefId() +
                                               " does not match expected ID " + expected);
        }
    }

    private Tree getTreeForRowDef(RowDef rowDef) {
        IndexDef indexDef = rowDef.getPKIndex().indexDef();
        assert indexDef != null : rowDef;
        try {
            treeService.populateTreeCache(indexDef);
            return indexDef.getTreeCache().getTree();
        }
        catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private class AccumulatorStatus implements TableStatus {
        private final int expectedID;
        private volatile AccumulatorAdapter ordinal;
        private volatile AccumulatorAdapter rowCount;
        private volatile AccumulatorAdapter uniqueID;
        private volatile AccumulatorAdapter autoIncrement;

        public AccumulatorStatus(int expectedID) {
            this.expectedID = expectedID;
        }

        @Override
        public long getAutoIncrement(Session session) {
            try {
                return autoIncrement.getSnapshot();
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        /** @deprecated Only used for 'upgrading' previous volumes as ordinal now lives in AIS */
        public int getOrdinal() {
            try {
                return (int) ordinal.getSnapshot();
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public long getRowCount(Session session) {
            try {
                return rowCount.getSnapshot();
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public void setRowCount(Session session, long rowCount) {
            try {
                internalSetRowCount(rowCount);
            }
            catch (PersistitInterruptedException e) {
                throw new PersistitAdapterException(e);
            }
        }

        @Override
        public long getApproximateRowCount() {
            return rowCount.getLiveValue();
        }

        @Override
        public long getUniqueID(Session session) {
            try {
                return uniqueID.getSnapshot();
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public long getApproximateUniqueID() {
            return uniqueID.getLiveValue();
        }

        @Override
        public int getTableID() {
            return expectedID;
        }

        @Override
        public void rowDeleted(Session session) {
            rowCount.sumAdd(-1);
        }

        @Override
        public void rowsWritten(Session session, long count) {
            rowCount.sumAdd(count);
        }

        @Override
        public long createNewUniqueID(Session session) {
            return uniqueID.seqAllocate();
        }

        @Override
        public void truncate(Session session) {
            try {
                internalSetRowCount(0);
                internalSetAutoIncrement(0, true);
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public void setAutoIncrement(Session session, long value) {
            internalSetAutoIncrement(value, false);
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            if(rowDef == null) {
                ordinal = rowCount = uniqueID = autoIncrement = null;
            } else {
                checkExpectedRowDefID(expectedID, rowDef);
                Tree tree = getTreeForRowDef(rowDef);
                ordinal = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ORDINAL, tree);
                rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree);
                uniqueID = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.UNIQUE_ID, tree);
                autoIncrement = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.AUTO_INC, tree);
            }
        }

        private void internalSetRowCount(long rowCountValue) throws PersistitInterruptedException {
            rowCount.set(rowCountValue);
        }

        private void internalSetAutoIncrement(long autoIncrementValue, boolean evenIfLess) {
            try {
                autoIncrement.set(autoIncrementValue, evenIfLess);
            } catch(PersistitInterruptedException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }
    }

    private class MemoryStatus implements TableStatus {
        private final int expectedID;
        private final MemoryTableFactory factory;

        private MemoryStatus(int expectedID, MemoryTableFactory factory) {
            this.expectedID = expectedID;
            this.factory = factory;
        }

        @Override
        public long getAutoIncrement(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getRowCount(Session session) {
            return factory.rowCount();
        }

        @Override
        public long getApproximateRowCount() {
            return factory.rowCount();
        }

        @Override
        public long getUniqueID(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getApproximateUniqueID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTableID() {
            return expectedID;
        }

        @Override
        public void setRowCount(Session session, long rowCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rowDeleted(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rowsWritten(Session session, long count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAutoIncrement(Session session, long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long createNewUniqueID(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            checkExpectedRowDefID(expectedID, rowDef);
        }
    }
}
