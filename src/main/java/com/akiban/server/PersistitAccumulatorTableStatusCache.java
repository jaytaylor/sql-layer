/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server;

import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.HashMap;
import java.util.Map;

public class PersistitAccumulatorTableStatusCache implements TableStatusCache {
    private Map<Integer, AccumulatorStatus> accumulatorStatuses = new HashMap<Integer, AccumulatorStatus>();
    private Map<Integer, MemoryStatus> memoryStatuses = new HashMap<Integer, MemoryStatus>();
    private TreeService treeService;

    public PersistitAccumulatorTableStatusCache(TreeService treeService) {
        this.treeService = treeService;
    }

    @Override
    public synchronized void rowDeleted(int tableID) {
        getModifiable(tableID).rowDeleted();
    }

    @Override
    public synchronized void rowWritten(int tableID) {
        getModifiable(tableID).rowWritten();
    }

    @Override
    public synchronized void truncate(int tableID) throws PersistitInterruptedException {
        getModifiable(tableID).truncate();
    }

    @Override
    public synchronized void drop(int tableID) throws PersistitInterruptedException {
        memoryStatuses.remove(tableID);
        AccumulatorStatus ts = accumulatorStatuses.remove(tableID);
        if(ts != null) {
            if(ts.autoIncrement != null) {
                ts.setAutoIncrement(0, true);
            }
            if(ts.rowCount != null) {
                ts.internalSetRowCount(0);
            }
            if(ts.ordinal != null) {
                ts.setOrdinal(0);
            }
            ts.setRowDef(null, null);
        }
    }

    @Override
    public synchronized void setAutoIncrement(int tableID, long value) throws PersistitInterruptedException {
        getModifiable(tableID).setAutoIncrement(value, false);
    }

    @Override
    public synchronized void setOrdinal(int tableID, int value) throws PersistitInterruptedException {
        getEither(tableID).setOrdinal(value);
    }

    @Override
    public synchronized void setRowDef(int tableID, RowDef rowDef) {
        InternalTableStatus ts = getEither(tableID);
        Tree tree = null;
        if(ts instanceof AccumulatorStatus) {
            tree = getTreeForRowDef(rowDef);
        }
        ts.setRowDef(rowDef, tree);
    }

    @Override
    public synchronized long createNewUniqueID(int tableID) throws PersistitInterruptedException {
        return getModifiable(tableID).createNewUniqueID();
    }

    @Override
    public synchronized TableStatus getTableStatus(int tableID) {
        AccumulatorStatus ts = accumulatorStatuses.get(tableID);
        if(ts == null) {
            ts = new AccumulatorStatus();
            accumulatorStatuses.put(tableID, ts);
        }
        return ts;
    }

    @Override
    public TableStatus getMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryStatus ts = memoryStatuses.get(tableID);
        if(ts == null) {
            if(factory == null) {
                throw new IllegalArgumentException("Null factory");
            }
            ts = new MemoryStatus(factory);
            memoryStatuses.put(tableID, ts);
        }
        return ts;
    }

    @Override
    public synchronized void detachAIS() {
        for(AccumulatorStatus ts : accumulatorStatuses.values()) {
            ts.setRowDef(null, null);
        }
        for(MemoryStatus ts : memoryStatuses.values()) {
            ts.setRowDef(null, null);
        }
    }
    
    //
    // Internal
    //

    private synchronized AccumulatorStatus getModifiable(int tableID) {
        AccumulatorStatus ts = accumulatorStatuses.get(tableID);
        if(ts == null) {
            final String errorMsg;
            if(memoryStatuses.containsKey(tableID)) {
                errorMsg = "Illegal operation on memory table: ";
            } else {
                errorMsg = "No such table status: ";
            }
            throw new IllegalArgumentException(errorMsg + tableID);
        }
        return ts;
    }

    private synchronized InternalTableStatus getEither(int tableID) {
        InternalTableStatus status = accumulatorStatuses.get(tableID);
        if(status != null) {
            return status;
        }
        status = memoryStatuses.get(tableID);
        if(status != null) {
            return status;
        }
        throw new IllegalArgumentException("No such status: " + tableID);
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

    private interface InternalTableStatus {
        void setOrdinal(int ordinal) throws PersistitInterruptedException;
        void setRowDef(RowDef rowDef, Tree tree);
    }

    private class AccumulatorStatus implements TableStatus, InternalTableStatus {
        private volatile RowDef rowDef;
        private volatile AccumulatorAdapter ordinal;
        private volatile AccumulatorAdapter rowCount;
        private volatile AccumulatorAdapter uniqueID;
        private volatile AccumulatorAdapter autoIncrement;

        @Override
        public long getAutoIncrement() throws PersistitInterruptedException {
            return autoIncrement.getSnapshot();
        }

        @Override
        public int getOrdinal() throws PersistitInterruptedException {
            return (int) ordinal.getSnapshot();
        }

        @Override
        public long getRowCount() throws PersistitInterruptedException {
            return rowCount.getSnapshot();
        }

        @Override
        public void setRowCount(long rowCount) {
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
        public long getUniqueID() throws PersistitInterruptedException {
            return uniqueID.getSnapshot();
        }

        @Override
        public RowDef getRowDef() {
            return rowDef;
        }

        void rowDeleted() {
            rowCount.updateAndGet(-1);
        }

        void rowWritten() {
            rowCount.updateAndGet(1);
        }

        void internalSetRowCount(long rowCountValue) throws PersistitInterruptedException {
            rowCount.set(rowCountValue);
        }

        void setAutoIncrement(long autoIncrementValue, boolean evenIfLess) throws PersistitInterruptedException {
            autoIncrement.set(autoIncrementValue, evenIfLess);
        }

        public void setOrdinal(int ordinal) throws PersistitInterruptedException {
            this.ordinal.set(ordinal);
        }

        public synchronized void setRowDef(RowDef rowDef, Tree tree) {
            this.rowDef = rowDef;
            if(rowDef == null && tree == null) {
                return;
            }
            ordinal = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ORDINAL, treeService, tree);
            rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, tree);
            uniqueID = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.UNIQUE_ID, treeService, tree);
            autoIncrement = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.AUTO_INC, treeService, tree);
        }
        
        long createNewUniqueID() throws PersistitInterruptedException {
            return uniqueID.updateAndGet(1);
        }

        void truncate() throws PersistitInterruptedException {
            internalSetRowCount(0);
            setAutoIncrement(0, true);
        }
    }

    private class MemoryStatus implements TableStatus, InternalTableStatus {
        private final MemoryTableFactory factory;
        private volatile RowDef rowDef;
        private volatile int ordinal;

        private MemoryStatus(MemoryTableFactory factory) {
            this.factory = factory;
        }

        @Override
        public long getAutoIncrement() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOrdinal() {
            return ordinal;
        }

        @Override
        public long getRowCount() {
            return factory.rowCount();
        }

        @Override
        public long getApproximateRowCount() {
            return factory.rowCount();
        }

        @Override
        public long getUniqueID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowDef getRowDef() {
            return rowDef;
        }

        @Override
        public void setRowCount(long rowCount) {
            throw new UnsupportedOperationException();
        }

        public void setOrdinal(int ordinal) {
            this.ordinal = ordinal;
        }

        public void setRowDef(RowDef rowDef, Tree tree) {
            this.rowDef = rowDef;
        }
    }
}
