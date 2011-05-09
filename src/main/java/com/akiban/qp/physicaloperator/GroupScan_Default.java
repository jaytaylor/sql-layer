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
import com.akiban.qp.row.Row;

class GroupScan_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName());
        buffer.append('(');
        buffer.append(groupTable.getName().getTableName());
        if (indexKeyRangeBindable != null) {
            buffer.append(" range");
        }
        if (reverse) {
            buffer.append(" reverse");
        }
        buffer.append(' ');
        buffer.append(limit);
        buffer.append(')');
        return buffer.toString();
    }

    // PhysicalOperator interface

    public Cursor cursor(StoreAdapter adapter, Bindings bindings)
    {
        Cursor cursor = new Execution(adapter, indexKeyRangeBindable.bindTo(bindings));
        assert cursor.cursorAbilitiesInclude(CursorAbility.MODIFY) : "cursor must be modifiable";
        return cursor;
    }

    @Override
    public boolean cursorAbilitiesInclude(CursorAbility ability) {
        return CursorAbility.MODIFY.equals(ability) || super.cursorAbilitiesInclude(ability);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupTable groupTable,
                             boolean reverse,
                             Limit limit,
                             Bindable<IndexKeyRange> indexKeyRangeBindable)
    {
        this.groupTable = groupTable;
        this.reverse = reverse;
        this.limit = limit;
        this.indexKeyRangeBindable = indexKeyRangeBindable;
    }

    // Object state

    private final GroupTable groupTable;
    private final boolean reverse;
    private final Limit limit;
    private final Bindable<IndexKeyRange> indexKeyRangeBindable;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
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
            checkHasRow();
            cursor.removeCurrentRow();
            outputRow(null);
        }

        @Override
        public void updateCurrentRow(Row newRow) {
            checkHasRow();
            cursor.updateCurrentRow(newRow);
            outputRow(newRow);
        }

        @Override
        public ModifiableCursorBackingStore backingStore() {
            return super.backingStore();
        }

        @Override
        public boolean cursorAbilitiesInclude(CursorAbility ability) {
            return cursor.cursorAbilitiesInclude(ability);
        }

        // private

        private void checkHasRow() {
            if (!hasCachedRow()) {
                throw new IllegalStateException("no cached row available");
            }
        }

        // Execution interface

        Execution(StoreAdapter adapter, IndexKeyRange indexKeyRange)
        {
            this.cursor = adapter.newGroupCursor(groupTable, reverse, indexKeyRange);
        }

        // Object state

        private final Cursor cursor;
    }
}
