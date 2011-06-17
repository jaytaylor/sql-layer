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
import com.akiban.util.ArgumentValidation;

class GroupScan_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' + cursorCreator + limit + ')';
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(cursorCreator.cursor(adapter), limit);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupCursorCreator cursorCreator, Limit limit)
    {
        ArgumentValidation.notNull("groupTable", cursorCreator);
        this.limit = limit;
        this.cursorCreator = cursorCreator;
    }

    // Object state

    private final Limit limit;
    private final GroupCursorCreator cursorCreator;

    // Inner classes

    private static class Execution extends SingleRowCachingCursor
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

        // Execution interface

        Execution(Cursor cursor, Limit limit)
        {
            this.cursor = cursor;
            this.limit = limit;
        }

        // Object state

        private final Cursor cursor;
        private final Limit limit;
    }

    static interface GroupCursorCreator {
        GroupCursor cursor(StoreAdapter adapter);
        GroupTable groupTable();
    }

    private static abstract class AbstractGroupCursorCreator implements GroupCursorCreator {

        // GroupCursorCreator interface

        @Override
        public final GroupTable groupTable() {
            return targetGroupTable;
        }


        // for use by subclasses

        protected AbstractGroupCursorCreator(GroupTable groupTable) {
            this.targetGroupTable = groupTable;
        }

        @Override
        public final String toString() {
            return describeRange() + " on " + targetGroupTable;
        }

        // for overriding in subclasses

        protected abstract String describeRange();

        private final GroupTable targetGroupTable;
    }

    static class FullGroupCursorCreator extends AbstractGroupCursorCreator {

        // GroupCursorCreator interface

        @Override
        public GroupCursor cursor(StoreAdapter adapter) {
            return adapter.newGroupCursor(groupTable());
        }

        // FullGroupCursorCreator interface

        public FullGroupCursorCreator(GroupTable groupTable) {
            super(groupTable);
        }

        // AbstractGroupCursorCreator interface

        @Override
        public String describeRange() {
            return "full scan";
        }
    }

    static class RangedGroupCursorCreator extends AbstractGroupCursorCreator  {

        // GroupCursorCreator interface

        @Override
        public GroupCursor cursor(StoreAdapter adapter) {
            return adapter.newGroupCursor(groupTable(), indexKeyRange);
        }

        // RangedGroupCursorCreator interface

        public RangedGroupCursorCreator(GroupTable groupTable, IndexKeyRange indexKeyRange) {
            super(groupTable);
            ArgumentValidation.notNull("range", indexKeyRange);
            this.indexKeyRange = indexKeyRange;
        }

        // AbstractGroupCursorCreator interface

        @Override
        public String describeRange() {
            return indexKeyRange.toString();
        }


        // object state

        private final IndexKeyRange indexKeyRange;
    }

    static class PositionalGroupCursorCreator extends AbstractGroupCursorCreator {

        // GroupCursorCreator interface

        @Override
        public GroupCursor cursor(StoreAdapter adapter) {
            GroupCursor cursor = adapter.newGroupCursor(groupTable());
            cursor.rebind(hKey, deep);
            return cursor;
        }

        // PositionalGroupCursorCreator interface

        PositionalGroupCursorCreator(GroupTable groupTable, HKey hKey, boolean deep) {
            super(groupTable);
            this.hKey = hKey;
            this.deep = deep;
        }

        // AbstractGroupCursorCreator interface

        @Override
        public String describeRange() {
            return (deep ? "shallow scan at " : "deep scan at ") + hKey;
        }

        // object state

        private final HKey hKey;
        private final boolean deep;
    }
}
