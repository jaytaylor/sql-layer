
package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;

public class MemoryGroupCursor implements GroupCursor {

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkIdle(this);
    }

    @Override
    public void open() {
        CursorLifecycle.checkIdle(this);
        scan = factory.getGroupScan(adapter);
        idle = false;
    }

    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        Row row = scan.next();
        if(row == null) {
            idle = true;
        }
        return row;
    }


    @Override
    public void close() {
        CursorLifecycle.checkIdleOrActive(this);
        if (!idle) {
            scan.close();
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

    @Override
    public void jump(Row row, ColumnSelector selector) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    // Abstraction extensions

    public interface GroupScan {
        /**
         * Get the next row from the stream.
         * @return The next row or <code>null</code> if none.
         */
        public Row next();

        /**
         * Clean up any state.
         */
        public void close();
    }
    
    public MemoryGroupCursor (MemoryAdapter adapter, Group group) {
        this.adapter = adapter;
        this.factory = group.getRoot().getMemoryTableFactory();
        assert this.factory != null : group;
    }
    
    private boolean idle = true;
    private boolean destroyed = false;
    private final MemoryAdapter adapter;
    private final MemoryTableFactory factory;
    private GroupScan scan;
}
