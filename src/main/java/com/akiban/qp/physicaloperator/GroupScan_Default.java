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

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;

class GroupScan_Default extends PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public Cursor cursor(StoreAdapter adapter, Bindings bindings) {
        return new Execution(adapter, hKeyBindable.bindTo(bindings), indexKeyRangeBindable.bindTo(bindings));
    }

    @Override
    public String toString() 
    {
        return String.format("%s(%s limit %s)", getClass().getSimpleName(), groupTable, limit);
    }


    // GroupScan_Default interface

    public GroupScan_Default(GroupTable groupTable, boolean reverse, Limit limit,
                             Bindable<HKey> hKeyBindable, Bindable<IndexKeyRange> indexKeyRangeBindable)
    {
        this.groupTable = groupTable;
        this.reverse = reverse;
        this.limit = limit;
        this.hKeyBindable = hKeyBindable;
        this.indexKeyRangeBindable = indexKeyRangeBindable;
    }

    // Object state

    private final GroupTable groupTable;
    private final boolean reverse;
    private final Limit limit;
    private final Bindable<HKey> hKeyBindable;
    private final Bindable<IndexKeyRange> indexKeyRangeBindable;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {

        // Cursor interface

        @Override
        public void open()
        {
            cursor.open();
        }

        @Override
        public boolean next()
        {
            boolean next = cursor.next();
            if (next) {
                Row row = cursor.currentRow();
                outputRow(row);
                if (limit.limitReached(row)) {
                    close();
                    next = false;
                }
            } else {
                close();
                next = false;
            }
            return next;
        }

        @Override
        public void close()
        {
            outputRow(null);
            cursor.close();
        }

        @Override
        public void removeCurrentRow() {
            cursor.removeCurrentRow();
            outputRow(null);
        }

        @Override
        public void updateCurrentRow(RowBase newRow) {
            cursor.updateCurrentRow(newRow);
            outputRow((Row)newRow); // TODO remove this cast
        }

        @Override
        public ModifiableCursorBackingStore backingStore() {
            return super.backingStore();
        }

        // Execution interface

        Execution(StoreAdapter adapter, HKey hKey, IndexKeyRange indexKeyRange)
        {
            this.cursor = adapter.newGroupCursor(groupTable, reverse, hKey, indexKeyRange);
        }

        // Object state

        private final Cursor cursor;
    }
}
