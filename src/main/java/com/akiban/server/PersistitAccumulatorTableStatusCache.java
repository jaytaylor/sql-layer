/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server;

import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.HashMap;
import java.util.Map;

import static com.persistit.Accumulator.Type;

public class PersistitAccumulatorTableStatusCache implements TableStatusCache {
    private Map<Integer, InternalTableStatus> tableStatusMap = new HashMap<Integer, InternalTableStatus>();
    private TreeService treeService;

    public PersistitAccumulatorTableStatusCache(TreeService treeService) {
        this.treeService = treeService;
    }

    @Override
    public synchronized void rowDeleted(int tableID) {
        getInternalTableStatus(tableID).rowDeleted();
    }

    @Override
    public synchronized void rowWritten(int tableID) {
        getInternalTableStatus(tableID).rowWritten();
    }

    @Override
    public synchronized void truncate(int tableID) {
        InternalTableStatus ts = getInternalTableStatus(tableID);
        ts.truncate();
    }

    @Override
    public synchronized void drop(int tableID) {
        InternalTableStatus ts = tableStatusMap.remove(tableID);
        if(ts != null) {
            ts.setAutoIncrement(0);
            ts.setRowCount(0);
            ts.setOrdinal(0);
            ts.setRowDef(null, null);
        }
    }

    @Override
    public synchronized void setAutoIncrement(int tableID, long value) {
        getInternalTableStatus(tableID).setAutoIncrement(value);
    }

    @Override
    public synchronized void setOrdinal(int tableID, int value) {
        getInternalTableStatus(tableID).setOrdinal(value);
    }

    @Override
    public void setRowDef(int tableID, RowDef rowDef) {
        InternalTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            throw new IllegalArgumentException("Unknown table ID " + tableID + " for RowDef " + rowDef);
        }
        Tree tree = getTreeForRowDef(rowDef);
        ts.setRowDef(rowDef, tree);
    }

    @Override
    public synchronized  long createNewUniqueID(int tableID) {
        try {
            return getInternalTableStatus(tableID).createNewUniqueID();
        } catch(PersistitInterruptedException e) {
            throw new PersistItErrorException(e);
        }
    }

    @Override
    public synchronized TableStatus getTableStatus(int tableID) {
        return getInternalTableStatus(tableID);
    }

    @Override
    public synchronized void loadAllInVolume(String volumeName) throws Exception {
        // Nothing to do
    }

    @Override
    public synchronized void detachAIS() {
        for(InternalTableStatus ts : tableStatusMap.values()) {
            ts.setRowDef(null, null);
        }
    }
    
    //
    // Internal
    //
    
    private synchronized InternalTableStatus getInternalTableStatus(int tableID) {
        InternalTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            ts = new InternalTableStatus();
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }
    
    private Tree getTreeForRowDef(RowDef rowDef) {
        IndexDef indexDef = (IndexDef) rowDef.getPKIndex().indexDef();
        assert indexDef != null : rowDef;
        try {
            treeService.populateTreeCache(indexDef);
            return indexDef.getTreeCache().getTree();
        }
        catch (PersistitException e) {
            throw new PersistItErrorException(e);
        }
    }


    /**
     * Mapping of indexes and types for the Accumulators used by the table status.
     * <p>
     * Note: Remember that <i>any</i> modification to existing values is an
     * <b>incompatible</b> data format change. It is only safe to stop using
     * an index position or add new ones at the end of the range.
     * </p>
     */
    private static enum AccumInfo {
        ORDINAL(0, Type.SUM),
        ROW_COUNT(1, Type.SUM),
        UNIQUE_ID(2, Type.SEQ),
        AUTO_INC(3, Type.SUM),
        ;

        AccumInfo(int index, Type type) {
            this.index = index;
            this.type = type;
        }
        
        int getIndex() {
            return index;
        }
        
        Type getType() {
            return type;
        }

        private final int index;
        private final Accumulator.Type type;
    }

    
    private class InternalTableStatus implements TableStatus {
        private volatile RowDef rowDef;
        private volatile Accumulator ordinal;
        private volatile Accumulator rowCount;
        private volatile Accumulator uniqueID;
        private volatile Accumulator autoIncrement;

        @Override
        public long getAutoIncrement() {
            return getSnapshot(autoIncrement, getCurrentTrx());
        }

        @Override
        public int getOrdinal() {
            return (int) getSnapshot(ordinal, getCurrentTrx());
        }

        @Override
        public long getRowCount() {
            return getSnapshot(rowCount, getCurrentTrx());
        }

        @Override
        public long getUniqueID() {
            return getSnapshot(uniqueID, getCurrentTrx());
        }

        @Override
        public RowDef getRowDef() {
            return rowDef;
        }

        void rowDeleted() {
            rowCount.update(-1, getCurrentTrx());
        }

        void rowWritten() {
            rowCount.update(1, getCurrentTrx());
        }

        void setRowCount(long rowCountValue) {
            long diff = rowCountValue - getSnapshot(rowCount, getCurrentTrx());
            this.rowCount.update(diff, getCurrentTrx());
        }

        void setAutoIncrement(long autoIncrementValue) {
            long diff = autoIncrementValue - getSnapshot(autoIncrement, getCurrentTrx());
            this.autoIncrement.update(diff, getCurrentTrx());
        }

        void setOrdinal(int ordinalValue) {
            long diff = ordinalValue - getSnapshot(ordinal, getCurrentTrx());
            this.ordinal.update(diff, getCurrentTrx());
        }

        synchronized void setRowDef(RowDef rowDef, Tree tree) {
            this.rowDef = rowDef;
            if(rowDef == null && tree == null) {
                return;
            }
            try {
                ordinal = getAccumulator(tree, AccumInfo.ORDINAL);
                rowCount = getAccumulator(tree, AccumInfo.ROW_COUNT);
                uniqueID = getAccumulator(tree, AccumInfo.UNIQUE_ID);
                autoIncrement = getAccumulator(tree, AccumInfo.AUTO_INC);
            } catch(PersistitException e) {
                throw new PersistItErrorException(e);
            }
        }
        
        long createNewUniqueID() throws PersistitInterruptedException {
            return this.uniqueID.update(1, getCurrentTrx());
        }

        void truncate() {
            setRowCount(0);
            setAutoIncrement(0);
        }

        private Transaction getCurrentTrx() {
            return treeService.getDb().getTransaction();
        }

        private Accumulator getAccumulator(Tree tree, AccumInfo info) throws PersistitException {
            return tree.getAccumulator(info.getType(), info.getIndex());
        }

        private long getSnapshot(Accumulator accumulator, Transaction txn) {
            try {
                return accumulator.getSnapshotValue(txn);
            } catch(PersistitInterruptedException e) {
                throw new PersistItErrorException(e);
            }
        }
    }
}
