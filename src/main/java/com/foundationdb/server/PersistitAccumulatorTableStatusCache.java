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
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.error.PersistitAdapterException;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;

import java.util.HashMap;
import java.util.Map;

public class PersistitAccumulatorTableStatusCache implements TableStatusCache {
    private Map<Integer,MemoryTableStatus> memoryStatuses = new HashMap<>();
    private TreeService treeService;

    public PersistitAccumulatorTableStatusCache(TreeService treeService) {
        this.treeService = treeService;
    }

    public synchronized void clearTableStatus(Session session, Table table) {
        // Nothing for the status itself, Accumulators attached to Tree
        memoryStatuses.remove(table.getTableId());
    }

    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new AccumulatorStatus(tableID);
    }

    @Override
    public synchronized TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryTableStatus ts = memoryStatuses.get(tableID);
        if(ts == null) {
            if(factory == null) {
                throw new IllegalArgumentException("Null factory");
            }
            ts = new MemoryTableStatus(tableID, factory);
            memoryStatuses.put(tableID, ts);
        }
        return ts;
    }

    @Override
    public synchronized void detachAIS() {
        for(MemoryTableStatus ts : memoryStatuses.values()) {
            ts.setRowDef(null);
        }
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
        return getTreeForIndex(rowDef.getPKIndex());
    }
    
    private Tree getTreeForIndex(TableIndex pkTableIndex) {
        PersistitStorageDescription storageDescription = (PersistitStorageDescription)pkTableIndex.getStorageDescription();
        try {
            treeService.populateTreeCache(storageDescription);
            return storageDescription.getTreeCache();
        }
        catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private class AccumulatorStatus implements TableStatus {
        private final int expectedID;
        private volatile AccumulatorAdapter rowCount;
        private volatile AccumulatorAdapter autoIncrement;

        public AccumulatorStatus(int expectedID) {
            this.expectedID = expectedID;
        }

        @Override
        public long getAutoIncrement(Session session) {
            try {
                return autoIncrement.getSnapshot();
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public long getRowCount(Session session) {
            try {
                return rowCount.getSnapshot();
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public void setRowCount(Session session, long rowCount) {
            try {
                internalSetRowCount(rowCount);
            }
            catch (PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        @Override
        public long getApproximateRowCount(Session session) {
            return rowCount.getLiveValue();
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
        public void truncate(Session session) {
            try {
                internalSetRowCount(0);
                internalSetAutoIncrement(0, true);
            } catch(PersistitException | RollbackException e) {
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
                rowCount = autoIncrement = null;
            } else {
                checkExpectedRowDefID(expectedID, rowDef);
                Tree tree = getTreeForRowDef(rowDef);
                rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree);
                autoIncrement = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.AUTO_INC, tree);
            }
        }
        
        @Override
        public void setIndex(TableIndex pkTableIndex) {
            if (pkTableIndex == null) {
                rowCount = autoIncrement = null;
            } else {
                assert pkTableIndex.getTable().getTableId().intValue() == expectedID;
                Tree tree = getTreeForIndex(pkTableIndex);
                rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree);
                autoIncrement = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.AUTO_INC, tree);
            }
        }

        private void internalSetRowCount(long rowCountValue) throws PersistitInterruptedException {
            rowCount.set(rowCountValue);
        }

        private void internalSetAutoIncrement(long autoIncrementValue, boolean evenIfLess) {
            try {
                autoIncrement.set(autoIncrementValue, evenIfLess);
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }
    }
}
