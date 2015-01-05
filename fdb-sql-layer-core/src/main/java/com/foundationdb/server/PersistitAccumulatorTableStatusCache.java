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
    public synchronized TableStatus createTableStatus (Table table) {
        return new AccumulatorStatus(table);
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
        // TODO: Nothing
    }

    //
    // Internal
    //

    private Tree getTreeForTable (Table table) {
        return getTreeForIndex(table.getPrimaryKeyIncludingInternal().getIndex());
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

        public AccumulatorStatus (Table table) {
            this.expectedID = table.getTableId();
            Tree tree = getTreeForTable(table);
            rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree);
        }

        public AccumulatorStatus(int expectedID) {
            this.expectedID = expectedID;
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
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(null, e);
            }
        }

        private void internalSetRowCount(long rowCountValue) throws PersistitInterruptedException {
            rowCount.set(rowCountValue);
        }
    }
}
