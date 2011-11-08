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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.util.ArgumentValidation;

class GroupScan_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' + cursorCreator + ')';
    }

    // Operator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, cursorCreator);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupCursorCreator cursorCreator)
    {
        ArgumentValidation.notNull("groupTable", cursorCreator);
        this.cursorCreator = cursorCreator;
    }

    // Object state

    private final GroupCursorCreator cursorCreator;

    // Inner classes

    private static class Execution extends OperatorExecutionBase implements Cursor
    {

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row;
            if ((row = cursor.next()) == null) {
                close();
                row = null;
            }
            return row;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter, GroupCursorCreator cursorCreator)
        {
            super(adapter);
            this.cursor = cursorCreator.cursor(adapter);
        }

        // Object state

        private final Cursor cursor;
    }

    static interface GroupCursorCreator
    {
        Cursor cursor(StoreAdapter adapter);

        GroupTable groupTable();
    }

    private static abstract class AbstractGroupCursorCreator implements GroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public final GroupTable groupTable()
        {
            return targetGroupTable;
        }


        // for use by subclasses

        protected AbstractGroupCursorCreator(GroupTable groupTable)
        {
            this.targetGroupTable = groupTable;
        }

        @Override
        public final String toString()
        {
            return describeRange() + " on " + targetGroupTable.getName().getTableName();
        }

        // for overriding in subclasses

        protected abstract String describeRange();

        private final GroupTable targetGroupTable;
    }

    static class FullGroupCursorCreator extends AbstractGroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public Cursor cursor(StoreAdapter adapter)
        {
            return adapter.newGroupCursor(groupTable());
        }

        // FullGroupCursorCreator interface

        public FullGroupCursorCreator(GroupTable groupTable)
        {
            super(groupTable);
        }

        // AbstractGroupCursorCreator interface

        @Override
        public String describeRange()
        {
            return "full scan";
        }
    }

    static class PositionalGroupCursorCreator extends AbstractGroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public Cursor cursor(StoreAdapter adapter)
        {
            return new HKeyBoundCursor(adapter, adapter.newGroupCursor(groupTable()), hKeyBindingPosition, deep, hKeyType, shortenUntil);
        }

        // PositionalGroupCursorCreator interface

        PositionalGroupCursorCreator(GroupTable groupTable, int hKeyBindingPosition, boolean deep, UserTable hKeyType, UserTable shortenUntil)
        {
            super(groupTable);
            this.hKeyBindingPosition = hKeyBindingPosition;
            this.deep = deep;
            if ((shortenUntil == hKeyType) || shortenUntil.isDescendantOf(hKeyType)) {
                shortenUntil = null;
                hKeyType = null;
            }
            this.shortenUntil = shortenUntil;
            this.hKeyType = hKeyType;
            assert (hKeyType == null) == (shortenUntil == null) : hKeyType + " ~ " + shortenUntil;
            assert hKeyType == null || hKeyType.isDescendantOf(shortenUntil)
                    : hKeyType + " is not a descendant of " + shortenUntil;
        }

        // AbstractGroupCursorCreator interface

        @Override
        public String describeRange()
        {
            return deep ? "deep hkey-bound scan" : "shallow hkey-bound scan";
        }

        // object state

        private final int hKeyBindingPosition;
        private final boolean deep;
        private final UserTable shortenUntil;
        private final UserTable hKeyType;
    }

    private static class HKeyBoundCursor extends ChainedCursor
    {

        @Override
        public void open(Bindings bindings)
        {
            this.bindings = bindings;
            HKey hKey = getHKeyFromBindings();
            input.rebind(hKey, deep);
            input.open(bindings);
        }

        @Override
        public Row next() {
            // If we've ever seen a row, just defer to super
            if (sawOne) {
                return super.next();
            }
            Row result = super.next();
            // If we saw a row, mark it as such and defer to super
            if (result != null) {
                sawOne = true;
                return result;
            }
            // Our search is at an end; return our answer
            if (atTable == null || atTable == stopSearchTable) {
                return null;
            }
            // Close the input, shorten our hkey, re-open and try again
            close();
            assert atTable.parentTable() != null : atTable;
            atTable = atTable.parentTable();
            HKey hkey = getHKeyFromBindings();
            hkey.useSegments(atTable.getDepth() + 1);
            open(bindings);
            return next();
        }

        HKeyBoundCursor(StoreAdapter adapter,
                        GroupCursor input,
                        int hKeyBindingPosition,
                        boolean deep,
                        UserTable hKeyType,
                        UserTable shortenUntil)
        {
            super(adapter, input);
            this.input = input;
            this.hKeyBindingPosition = hKeyBindingPosition;
            this.deep = deep;
            this.atTable = hKeyType;
            this.stopSearchTable = shortenUntil;
        }

        private int[] hKeyDepths(UserTable hKeyType, UserTable shortenUntil) {
            int[] result = new int[shortenUntil.getDepth() - hKeyType.getDepth() + 1];
            int i = 0;
            for(UserTable curr = hKeyType; curr != null && curr != shortenUntil.parentTable(); curr = curr.parentTable()) {
                result[i] = curr.getDepth() + 1;
            }
            return result;
        }

        private HKey getHKeyFromBindings() {
            Object supposedHKey = bindings.get(hKeyBindingPosition);
            if (!(supposedHKey instanceof HKey)) {
                throw new RuntimeException(String.format("%s doesn't contain hkey at position %s",
                        bindings, hKeyBindingPosition));
            }
            return (HKey) supposedHKey;
        }

        private final GroupCursor input;
        private final int hKeyBindingPosition;
        private final boolean deep;
        private UserTable atTable;
        private final UserTable stopSearchTable;
        private boolean sawOne = false;
        private Bindings bindings;
    }
}
