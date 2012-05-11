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

import com.akiban.server.rowdata.RowDef;

import java.util.HashMap;
import java.util.Map;

public class MemoryOnlyTableStatusCache implements TableStatusCache {
    private final Map<Integer, InternalTableStatus> tableStatusMap = new HashMap<Integer, InternalTableStatus>();
            
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
        getInternalTableStatus(tableID).truncate();
    }

    @Override
    public synchronized void drop(int tableID) {
        tableStatusMap.remove(tableID);
    }

    @Override
    public synchronized void setAutoIncrement(int tableID, long value) {
        getInternalTableStatus(tableID).setAutoIncrement(value);
    }

    @Override
    public synchronized long createNewUniqueID(int tableID) {
        return getInternalTableStatus(tableID).createNewUniqueID();
    }

    @Override
    public synchronized void setOrdinal(int tableID, int value) {
        getInternalTableStatus(tableID).setOrdinal(value);
    }

    @Override
    public void setRowDef(int tableID, RowDef rowDef) {
        getInternalTableStatus(tableID).setRowDef(rowDef);
    }

    @Override
    public synchronized TableStatus getTableStatus(int tableID) {
        return getInternalTableStatus(tableID);
    }

    @Override
    public void loadAllInVolume(String volumeName) throws Exception {
        // Nothing persisted
    }

    @Override
    public synchronized void detachAIS() {
        for(Map.Entry<Integer, InternalTableStatus> entry : tableStatusMap.entrySet()) {
            entry.getValue().setRowDef(null);
        }
    }


    private InternalTableStatus getInternalTableStatus(int tableID) {
        InternalTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            ts = new InternalTableStatus();
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }

    private static class InternalTableStatus implements TableStatus {
        private long autoIncrement = 0;
        private int ordinal = 0;
        private long rowCount = 0;
        private long uniqueID = 0;
        private RowDef rowDef = null;

                
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
            return rowCount;
        }

        @Override
        public synchronized void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public synchronized long getApproximateRowCount() {
            return rowCount;
        }

        @Override
        public synchronized long getUniqueID() {
            return uniqueID;
        }

        @Override
        public synchronized RowDef getRowDef() {
            return rowDef;
        }

        synchronized void setRowDef(RowDef rowDef) {
            this.rowDef = rowDef;
        }

        synchronized void rowDeleted() {
            rowCount = Math.max(0, rowCount - 1);
        }

        synchronized void rowWritten() {
            ++rowCount;
        }

        synchronized void setAutoIncrement(long autoIncrement) {
            this.autoIncrement = Math.max(this.autoIncrement, autoIncrement);
        }

        synchronized void setOrdinal(int ordinal) {
            this.ordinal = ordinal;
        }

        synchronized void setUniqueID(long uniqueID) {
            this.uniqueID = Math.max(uniqueID, uniqueID);
        }

        synchronized long createNewUniqueID() {
            return ++uniqueID;
        }

        synchronized void truncate() {
            autoIncrement = 0;
            uniqueID = 0;
            rowCount = 0;
        }
    }
}
