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

package com.akiban.qp.operator;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import java.util.Map;

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

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: GroupScan_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: GroupScan_Default next");

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

    private static class Execution extends OperatorExecutionBase implements Cursor
    {

        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                cursor.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                checkQueryCancelation();
                Row row;
                if ((row = cursor.next()) == null) {
                    close();
                    row = null;
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        @Override
        public void destroy()
        {
            cursor.destroy();
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
        public boolean isDestroyed()
        {
            return cursor.isDestroyed();
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
            return describeRange() + " on " + targetGroup.getGroupTable().getName().getTableName();
        }

        // for overriding in subclasses

        private final Group targetGroup;
    }

    static class FullGroupCursorCreator extends AbstractGroupCursorCreator
    {

        // GroupCursorCreator interface

        @Override
        public Cursor cursor(QueryContext context)
        {
            return context.getStore(group().getGroupTable().getRoot()).newGroupCursor(group());
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
        public Cursor cursor(QueryContext context)
        {
            return new HKeyBoundCursor(context, 
                    context.getStore(group().getGroupTable().getRoot()).newGroupCursor(group()),
                    hKeyBindingPosition, 
                    deep, 
                    hKeyType, 
                    shortenUntil);
        }

        // PositionalGroupCursorCreator interface

        PositionalGroupCursorCreator(Group group,
                                     int hKeyBindingPosition,
                                     boolean deep,
                                     UserTable hKeyType,
                                     UserTable shortenUntil)
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
