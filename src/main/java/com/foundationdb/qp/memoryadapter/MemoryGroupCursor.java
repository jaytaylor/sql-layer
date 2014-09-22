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

package com.foundationdb.qp.memoryadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;

public class MemoryGroupCursor extends RowCursorImpl implements GroupCursor {

    @Override
    public void rebind(HKey hKey, boolean deep) {
        CursorLifecycle.checkIdle(this);
    }

    @Override
    public void open() {
        super.open();
        scan = factory.getGroupScan(adapter);
    }

    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        Row row = scan.next();
        if(row == null) {
            setIdle();
        }
        return row;
    }


    @Override
    public void close() {
        super.close();
        scan.close();
        scan = null;
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
        this.factory = MemoryAdapter.getMemoryTableFactory(group);
        assert this.factory != null : group;
    }
    
    private final MemoryAdapter adapter;
    private final MemoryTableFactory factory;
    private GroupScan scan;
}
