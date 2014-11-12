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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 <h1>Overview</h1>

 GroupScan_Default scans a group in hkey order.

 <h1>Arguments</h1>

 <ul>

 <li><b>GroupTable groupTable:</b>
 The group table to be scanned.

 <li><b>Limit limit (DEPRECATED):</b>
 A limit on the number of rows to be returned. The limit is specific to one Table.
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
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor, cursorCreator);
    }

    // GroupScan_Default interface

    public GroupScan_Default(GroupCursorCreator cursorCreator)
    {
        ArgumentValidation.notNull("groupTable", cursorCreator);
        this.cursorCreator = cursorCreator;
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: GroupScan_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: GroupScan_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(GroupScan_Default.class);

    // Object state

    private final GroupCursorCreator cursorCreator;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        att.put(Label.SCAN_OPTION, PrimitiveExplainer.getInstance(cursorCreator.describeRange()));
        TableName rootName = cursorCreator.group().getRoot().getName();
        att.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(rootName.getSchemaName()));
        att.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(rootName.getTableName()));
        return new CompoundExplainer(Type.SCAN_OPERATOR, att);
    }

    // Inner classes

    private static class Execution extends LeafCursor implements Rebindable
    {

        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                cursor.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                checkQueryCancelation();
                Row row;
                if ((row = cursor.next()) == null) {
                    setIdle();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("GroupScan_Default: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            cursor.close();
            super.close();
        }

        @Override
        public boolean isIdle()
        {
            return cursor.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return cursor.isActive();
        }

        @Override
        public QueryBindings nextBindings() {
            QueryBindings bindings = super.nextBindings();
            if (cursor instanceof BindingsAwareCursor)
                ((BindingsAwareCursor)cursor).rebind(bindings);
            return bindings;
        }

        @Override
        public void rebind(HKey hKey, boolean deep) {
            if(!canRebind) {
                throw new IllegalStateException("rebind not allowed for");
            }
            cursor.rebind(hKey, deep);
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor, GroupCursorCreator cursorCreator)
        {
            super(context, bindingsCursor);
            this.cursor = cursorCreator.cursor(context);
            this.canRebind = (cursorCreator instanceof FullGroupCursorCreator);
        }

        // Object state

        private final GroupCursor cursor;
        private final boolean canRebind;
    }

    static interface GroupCursorCreator
    {
        GroupCursor cursor(QueryContext context);

        Group group();
        
        String describeRange();
    }

    private static abstract class AbstractGroupCursorCreator implements GroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public final Group group()
        {
            return targetGroup;
        }


        // for use by subclasses

        protected AbstractGroupCursorCreator(Group group)
        {
            this.targetGroup = group;
        }

        @Override
        public final String toString()
        {
            return describeRange() + " on " + targetGroup.getRoot().getName();
        }

        // for overriding in subclasses

        private final Group targetGroup;
    }

    static class FullGroupCursorCreator extends AbstractGroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public GroupCursor cursor(QueryContext context)
        {
            return context.getStore(group().getRoot()).newGroupCursor(group());
        }

        // FullGroupCursorCreator interface

        public FullGroupCursorCreator(Group group)
        {
            super(group);
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
        public GroupCursor cursor(QueryContext context)
        {
            return new HKeyBoundCursor(context,
                    context.getStore(group().getRoot()).newGroupCursor(group()),
                    hKeyBindingPosition, 
                    deep, 
                    hKeyType, 
                    shortenUntil);
        }

        // PositionalGroupCursorCreator interface

        PositionalGroupCursorCreator(Group group,
                                     int hKeyBindingPosition,
                                     boolean deep,
                                     Table hKeyType,
                                     Table shortenUntil)
        {
            super(group);
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
        private final Table shortenUntil;
        private final Table hKeyType;
    }

    private static class HKeyBoundCursor extends RowCursorImpl implements BindingsAwareCursor, GroupCursor
    {

        @Override
        public void open()
        {
            super.open();
            HKey hKey = getHKeyFromBindings();
            input.rebind(hKey, deep);
            input.open();
        }

        @Override
        public Row next() {
            // If we've ever seen a row, just defer to input
            if (sawOne) {
                return input.next();
            }
            Row result = input.next();
            // If we saw a row, mark it as such and defer to input
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
            assert atTable.getParentTable() != null : atTable;
            atTable = atTable.getParentTable();
            HKey hkey = getHKeyFromBindings();
            hkey.useSegments(atTable.getDepth() + 1);
            open();
            return next();
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            input.jump(row, columnSelector);
            state = CursorLifecycle.CursorState.ACTIVE;
        }

        @Override
        public void close() {
            input.close();
            super.close();
        }

        @Override
        public void rebind(QueryBindings bindings) {
            this.bindings = bindings;
        }

        @Override
        public void rebind(HKey hKey, boolean deep) {
            throw new UnsupportedOperationException();
        }

        HKeyBoundCursor(QueryContext context,
                        GroupCursor input,
                        int hKeyBindingPosition,
                        boolean deep,
                        Table hKeyType,
                        Table shortenUntil)
        {
            this.context = context;
            this.input = input;
            this.hKeyBindingPosition = hKeyBindingPosition;
            this.deep = deep;
            this.atTable = hKeyType;
            this.stopSearchTable = shortenUntil;
        }

        private HKey getHKeyFromBindings() {
            return bindings.getHKey(hKeyBindingPosition);
        }

        private final QueryContext context;
        private final GroupCursor input;
        private final int hKeyBindingPosition;
        private final boolean deep;
        private Table atTable;
        private final Table stopSearchTable;
        private boolean sawOne = false;
        private QueryBindings bindings;
    }
}
