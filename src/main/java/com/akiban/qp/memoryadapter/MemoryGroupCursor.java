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

package com.akiban.qp.memoryadapter;

import java.util.Iterator;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;

public class MemoryGroupCursor implements GroupCursor {

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkIdle(this);
    }

    @Override
    public void open() {
        CursorLifecycle.checkIdle(this);
        scan = factory.getTableScan(getRowType());
        idle = false;
    }

    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        Row row = null; 
        
        if (scan.hasNext()) {
            row = scan.next();
        } else {
            idle = true;
        }
        
        return row;
    }


    @Override
    public void close() {
        CursorLifecycle.checkIdleOrActive(this);
        if (!idle) {
            
            memoryClose();
            scan = null;
            idle = true;
        }
    }

    @Override
    public void destroy() {
        destroyed = true;
    }

    @Override
    public boolean isActive() {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public boolean isIdle() {
        return !destroyed && idle;
    }

    // Abstraction extensions

    /**
     * create the TableScan implementation specific to your code 
     */
    //public abstract TableScan memoryOpen();
    
    public static abstract class TableScan implements Iterator<Row> {
        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Close the cursor processing, if any. 
     */
    public void memoryClose() {}

    /**
     * Create a new row for this table. 
     * @return
     */
    protected ValuesHolderRow newRow() {
        return new ValuesHolderRow (getRowType());
    }
    
    // Package use

    private RowType getRowType() {
        return adapter.schema().userTableRowType(table.getRoot());
    }

    public MemoryGroupCursor (MemoryAdapter adapter, GroupTable groupTable, MemoryTableFactory factory) {
        this.adapter = adapter;
        this.table = groupTable;
        this.factory = factory;
    }
    
    private boolean idle = true;
    private boolean destroyed = false;
    private final MemoryAdapter adapter;
    private final GroupTable table;
    private final MemoryTableFactory factory;
    private TableScan scan;
}
