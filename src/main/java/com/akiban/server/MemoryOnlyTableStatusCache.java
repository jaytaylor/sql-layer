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
import com.akiban.server.rowdata.RowDef;

import java.util.HashMap;
import java.util.Map;

public class MemoryOnlyTableStatusCache implements TableStatusCache {
    private final Map<Integer, InternalTableStatus> tableStatusMap = new HashMap<Integer, InternalTableStatus>();
            
    @Override
    public synchronized TableStatus getTableStatus(int tableID) {
        return new InternalTableStatus(null);
    }

    @Override
    public TableStatus getMemoryTableStatus(int tableID, MemoryTableFactory factory) {
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
            ts = new InternalTableStatus(factory);
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }

    private static class InternalTableStatus implements TableStatus {
        private final MemoryTableFactory factory;
        private long autoIncrement = 0;
        private int ordinal = 0;
        private long rowCount = 0;
        private long uniqueID = 0;
        private RowDef rowDef = null;

        public InternalTableStatus(MemoryTableFactory factory) {
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
        public synchronized RowDef getRowDef() {
            return rowDef;
        }

        @Override
        public synchronized void setRowDef(RowDef rowDef) {
            this.rowDef = rowDef;
        }

        @Override
        public synchronized void rowDeleted() {
            rowCount = Math.max(0, rowCount - 1);
        }

        @Override
        public synchronized void rowWritten() {
            ++rowCount;
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
