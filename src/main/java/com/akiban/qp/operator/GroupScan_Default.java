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
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;

/**

 <h1>Overview</h1>

 GroupScan_Default scans a group in hkey order.

 <h1>Arguments</h1>

 <ul>

 <li><b>GroupTable groupTable:</b>
 The group table to be scanned.

 <li><b>Limit limit (DEPRECATED):</b>
 A limit on the number of rows to be returned. The limit is specific to one UserTable.
 Deprecated because the result is not well-defined. In the case of a branching group, a
 limit on one sibling has impliciations on the return of rows of other siblings.

 <li>IndexKeyRange indexKeyRange (DEPRECATED):</b> Specifies an index
 restriction for hkey-equivalent indexes. Deprecated because
 hkey-equivalent indexes were used automatically, sometimes reducing
 performance. Need to revisit the concept.

 <ul>

 <h1>Behavior</h1>

 The rows of a group table are returned in hkey order.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 GroupScan_Default does a complete scan of a group table, relying on nothing but sequential access.

 <h1>Memory Requirements</h1>

 None.

 */

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
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, cursorCreator);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupCursorCreator cursorCreator)
    {
        ArgumentValidation.notNull("groupTable", cursorCreator);
        this.cursorCreator = cursorCreator;
    }
    
    // Class state
    
    private static final PointTap GROUP_SCAN_COUNT = Tap.createCount("operator: group_scan", true);

    // Object state

    private final GroupCursorCreator cursorCreator;

    // Inner classes

    private static class Execution extends OperatorExecutionBase implements Cursor
    {

        // Cursor interface

        @Override
        public void open()
        {
            GROUP_SCAN_COUNT.hit();
            cursor.open();
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

        Execution(QueryContext context, GroupCursorCreator cursorCreator)
        {
            super(context);
            this.cursor = cursorCreator.cursor(context);
        }

        // Object state

        private final Cursor cursor;
    }

    static interface GroupCursorCreator
    {
        Cursor cursor(QueryContext context);

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
        public Cursor cursor(QueryContext context)
        {
            return context.getStore().newGroupCursor(groupTable());
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
        public Cursor cursor(QueryContext context)
        {
            return new HKeyBoundCursor(context, context.getStore().newGroupCursor(groupTable()), hKeyBindingPosition, deep, hKeyType, shortenUntil);
        }

        // PositionalGroupCursorCreator interface

        PositionalGroupCursorCreator(GroupTable groupTable,
                                     int hKeyBindingPosition,
                                     boolean deep,
                                     UserTable hKeyType,
                                     UserTable shortenUntil)
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
        public void open()
        {
            HKey hKey = getHKeyFromBindings();
            input.rebind(hKey, deep);
            input.open();
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
            open();
            return next();
        }

        HKeyBoundCursor(QueryContext context,
                        GroupCursor input,
                        int hKeyBindingPosition,
                        boolean deep,
                        UserTable hKeyType,
                        UserTable shortenUntil)
        {
            super(context, input);
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
            return context.getHKey(hKeyBindingPosition);
        }

        private final GroupCursor input;
        private final int hKeyBindingPosition;
        private final boolean deep;
        private UserTable atTable;
        private final UserTable stopSearchTable;
        private boolean sawOne = false;
    }
}
