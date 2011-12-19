package com.akiban.server;

import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Accumulator;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.HashMap;
import java.util.Map;

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
    public synchronized void rowUpdated(int tableID) {
    }

    @Override
    public synchronized void rowWritten(int tableID) {
        getInternalTableStatus(tableID).rowWritten();
    }

    @Override
    public synchronized void truncate(int tableID) {
    }

    @Override
    public synchronized void drop(int tableID) {
    }

    @Override
    public synchronized void setAutoIncrement(int tableID, long value) {
        try {
            getInternalTableStatus(tableID).setAutoIncrement(value);
        } catch(PersistitInterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void setOrdinal(int tableID, int value) {
        try {
            getInternalTableStatus(tableID).setOrdinal(value);
        } catch(PersistitInterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setRowDef(int tableID, RowDef rowDef) {
        InternalTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            throw new IllegalArgumentException("Unknown table ID " + tableID + " for RowDef " + rowDef);
        }
        
        try {
            IndexDef indexDef = (IndexDef) rowDef.getPKIndex().indexDef();
            treeService.populateTreeCache(indexDef);
            ts.setRowDef(rowDef, indexDef.getTreeCache().getTree());
        } catch(PersistitException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized  long createNewUniqueID(int tableID) {
        long uniqueID = 0;
        try {
            uniqueID = getInternalTableStatus(tableID).createNewUniqueID();
        } catch(PersistitInterruptedException e) {
            e.printStackTrace();
        }
        return uniqueID;
    }

    @Override
    public synchronized TableStatus getTableStatus(int tableID) {
        return getInternalTableStatus(tableID);
    }

    @Override
    public synchronized void loadAllInVolume(String volumeName) throws Exception {
    }

    @Override
    public synchronized void detachAIS() {
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
    

    private Transaction getCurrentTrx() {
        return treeService.getDb().getTransaction();
    }

    private class InternalTableStatus implements TableStatus {
        private volatile RowDef rowDef;
        private volatile Accumulator rowCount;
        private volatile Accumulator ordinal;
        private volatile Accumulator uniqueID;
        private volatile Accumulator autoIncrement;

        @Override
        public long getAutoIncrement() {
            try {
                return autoIncrement.getSnapshotValue(getCurrentTrx());
            } catch(PersistitInterruptedException e) {
                e.printStackTrace();
            }
            return 0;
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public long getLastDeleteTime() {
            return 0;
        }

        @Override
        public long getLastUpdateTime() {
            return 0;
        }

        @Override
        public long getLastWriteTime() {
            return 0;
        }

        @Override
        public int getOrdinal() {
            try {
                return (int) ordinal.getSnapshotValue(getCurrentTrx());
            } catch(PersistitInterruptedException e) {
                e.printStackTrace();
            }
            return 0;
        }

        @Override
        public long getRowCount() {
            try {
                return rowCount.getSnapshotValue(getCurrentTrx());
            } catch(PersistitInterruptedException e) {
                e.printStackTrace();
            }
            return 0;
        }

        @Override
        public long getUniqueID() {
            try {
                return uniqueID.getSnapshotValue(getCurrentTrx());
            } catch(PersistitInterruptedException e) {
                e.printStackTrace();
            }
            return 0;
        }

        @Override
        public RowDef getRowDef() {
            return rowDef;
        }

        synchronized void rowDeleted() {
            rowCount.update(-1, getCurrentTrx());
        }

        synchronized void rowUpdated() {
        }

        synchronized void rowWritten() {
            rowCount.update(1, getCurrentTrx());
        }

        synchronized void setAutoIncrement(long autoIncrement) throws PersistitInterruptedException {
            long diff = autoIncrement - this.autoIncrement.getSnapshotValue(getCurrentTrx());
            this.autoIncrement.update(diff, getCurrentTrx());
        }

        synchronized void setOrdinal(int ordinal) throws PersistitInterruptedException {
            long diff = ordinal - this.ordinal.getSnapshotValue(getCurrentTrx());
            this.ordinal.update(diff, getCurrentTrx());
        }

        synchronized void setRowDef(RowDef rowDef, Tree tree) {
            this.rowDef = rowDef;
            try {
                rowCount = tree.getAccumulator(Accumulator.Type.SUM, 0);
                ordinal = tree.getAccumulator(Accumulator.Type.MAX, 1);
                uniqueID = tree.getAccumulator(Accumulator.Type.SEQ, 2);
                autoIncrement = tree.getAccumulator(Accumulator.Type.MAX, 3);
            } catch(PersistitException e) {
                e.printStackTrace();
            }
        }
        
        synchronized long createNewUniqueID() throws PersistitInterruptedException {
            return this.uniqueID.update(1, getCurrentTrx());
        }

        synchronized void truncate() {
        }
    }
}
