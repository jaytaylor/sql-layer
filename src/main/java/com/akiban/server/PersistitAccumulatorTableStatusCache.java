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
    private Map<Integer, MemoryStatus> memoryStatuses = new HashMap<Integer, MemoryStatus>();
    private TreeService treeService;

    public PersistitAccumulatorTableStatusCache(TreeService treeService) {
        this.treeService = treeService;
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
        public int getTableID() {
            return expectedID;
        }

        @Override
        public void rowDeleted() {
            rowCount.updateAndGet(-1);
        }

        @Override
        public void rowWritten() {
            rowCount.updateAndGet(1);
        }

        public void setOrdinal(int ordinal) throws PersistitInterruptedException {
            this.ordinal.set(ordinal);
        }

        @Override
        public long createNewUniqueID() throws PersistitInterruptedException {
            return uniqueID.updateAndGet(1);
        }

        @Override
        public void truncate() throws PersistitInterruptedException {
            internalSetRowCount(0);
            internalSetAutoIncrement(0, true);
        }

        @Override
        public void setAutoIncrement(long value) throws PersistitInterruptedException {
            internalSetAutoIncrement(value, false);
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            if(rowDef == null) {
                ordinal = rowCount = uniqueID = autoIncrement = null;
            } else {
                checkExpectedRowDefID(expectedID, rowDef);
                Tree tree = getTreeForRowDef(rowDef);
                ordinal = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ORDINAL, treeService, tree);
                rowCount = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, tree);
                uniqueID = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.UNIQUE_ID, treeService, tree);
                autoIncrement = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.AUTO_INC, treeService, tree);
            }
        }

        private void internalSetRowCount(long rowCountValue) throws PersistitInterruptedException {
            rowCount.set(rowCountValue);
        }

        private void internalSetAutoIncrement(long autoIncrementValue, boolean evenIfLess) throws PersistitInterruptedException {
            autoIncrement.set(autoIncrementValue, evenIfLess);
        }
    }

    private class MemoryStatus implements TableStatus {
        private final int expectedID;
        private final MemoryTableFactory factory;
        private volatile int ordinal;

        private MemoryStatus(int expectedID, MemoryTableFactory factory) {
            this.expectedID = expectedID;
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
        public int getTableID() {
            return expectedID;
        }

        @Override
        public void setRowCount(long rowCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rowDeleted() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rowWritten() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAutoIncrement(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long createNewUniqueID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOrdinal(int ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public void setRowDef(RowDef rowDef) {
            checkExpectedRowDefID(expectedID, rowDef);
        }
    }
}
