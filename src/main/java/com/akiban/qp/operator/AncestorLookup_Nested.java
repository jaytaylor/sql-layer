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
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.std.LookUpOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 AncestorLookup_Nested locates ancestors of both group rows and index rows. Ancestors are located 
 using the hkey in a row of the current query's QueryContext.

 One expected usage is to locate the group row corresponding to an
 index row. For example, an index on customer.name yields index rows
 which AncestorLookup_Default can then use to locate customer
 rows. (The ancestor relationship is reflexive, e.g. customer is
 considered to be an ancestor of customer.)

 Another expected usage is to locate ancestors higher in the group. For
 example, given either an item row or an item index row,
 AncestorLookup_Default can be used to find the corresponding order and
 customer.

 AncestorLookup_Nested always locates 0-1 row per ancestor type.

 <h1>Arguments</h1>

 <ul>

 <li><b>GroupTable groupTable:</b> The group table containing the
 ancestors of interest.

 <li><b>RowType rowType:</b> Ancestors will be located for input rows
 of this type.

 <li><b>List<RowType> ancestorTypes:</b> Ancestor types to be located.

 <li><b>int inputBindingPosition:</b> Indicates input row's position in the query context. The hkey
 of this row will be used to locate ancestors.

 </ul>

 rowType may be an index row type or a group row type. For a group row
 type, rowType must not be one of the ancestorTypes. For an index row
 type, rowType may be one of the ancestorTypes.

 The groupTable, rowType, and all ancestorTypes must belong to the same
 group.

 Each ancestorType must be an ancestor of the rowType (or, if rowType
 is an index type, then an ancestor of the index's table's type).

 <h1>Behavior</h1>
 
 When this operator's cursor is opened, the row at position inputBindingPosition in the
 query context is accessed. The hkey from this row is obtained. For each ancestor type, the
 hkey is shortened if necessary, and the groupTable is then searched for
 a record with that exact hkey. All the retrieved records are written
 to the output stream in hkey order (ancestors before descendents), as
 is the input row if keepInput is true.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 For each input row, AncestorLookup_Default does one random access for
 each ancestor type.

 <h1>Memory Requirements</h1>

 AncestorLookup_Default stores in memory up to (ancestorTypes.size() +
 1) rows.

 */

class AncestorLookup_Nested extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), rowType, ancestorTypes);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public String describePlan()
    {
        return toString();
    }

    // AncestorLookup_Default interface

    public AncestorLookup_Nested(GroupTable groupTable,
                                 RowType rowType,
                                 Collection<? extends RowType> ancestorTypes,
                                 int inputBindingPosition)
    {
        ArgumentValidation.notNull("groupTable", groupTable);
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notNull("ancestorTypes", ancestorTypes);
        ArgumentValidation.notEmpty("ancestorTypes", ancestorTypes);
        ArgumentValidation.isTrue("inputBindingPosition >= 0", inputBindingPosition >= 0);
        // Keeping index rows not currently supported
        boolean inputFromIndex = rowType instanceof IndexRowType;
        RowType tableRowType =
            inputFromIndex
            ? ((IndexRowType) rowType).tableType()
            : rowType;
        // Each ancestorType must be an ancestor of rowType. ancestorType = tableRowType is OK only if the input
        // is from an index. I.e., this operator can be used for an index lookup.
        for (RowType ancestorType : ancestorTypes) {
            ArgumentValidation.isTrue("inputFromIndex || ancestorType1 != tableRowType",
                                      inputFromIndex || ancestorType != tableRowType);
            ArgumentValidation.isTrue("ancestorType.ancestorOf(tableRowType)",
                                      ancestorType.ancestorOf(tableRowType));
            ArgumentValidation.isTrue("ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup()",
                                      ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup());
        }
        this.groupTable = groupTable;
        this.rowType = rowType;
        this.inputBindingPosition = inputBindingPosition;
        // Sort ancestor types by depth
        this.ancestorTypes = new ArrayList<RowType>(ancestorTypes);
        if (this.ancestorTypes.size() > 1) {
            Collections.sort(this.ancestorTypes,
                             new Comparator<RowType>()
                             {
                                 @Override
                                 public int compare(RowType x, RowType y)
                                 {
                                     UserTable xTable = x.userTable();
                                     UserTable yTable = y.userTable();
                                     return xTable.getDepth() - yTable.getDepth();
                                 }
                             });
        }
        this.ancestorTypeDepth = new int[ancestorTypes.size()];
        int a = 0;
        for (RowType ancestorType : this.ancestorTypes) {
            this.ancestorTypeDepth[a++] = ancestorType.userTable().getDepth() + 1;
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(AncestorLookup_Nested.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: AncestorLookup_Nested open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: AncestorLookup_Nested next");

    // Object state

    private final GroupTable groupTable;
    private final RowType rowType;
    private final List<RowType> ancestorTypes;
    private final int[] ancestorTypeDepth;
    private final int inputBindingPosition;

    @Override
    public Explainer getExplainer()
    {
       OperationExplainer ex = new LookUpOperatorExplainer("Ancestor Lookup Nested", groupTable, rowType, null, null);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                Row rowFromBindings = context.getRow(inputBindingPosition);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("AncestorLookup_Nested: open using {}", rowFromBindings);
                }
                assert rowFromBindings.rowType() == rowType : rowFromBindings;
                rowFromBindings.hKey().copyTo(hKey);
                findAncestors();
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
                Row row = pending.take();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("AncestorLookup: {}", row == null ? null : row);
                }
                if (row == null) {
                    close();
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            pending.clear();
            ancestorCursor.close();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.pending = new PendingRows(ancestorTypeDepth.length);
            this.ancestorCursor = adapter().newGroupCursor(groupTable);
            this.hKey = adapter().newHKey(rowType);
        }

        // For use by this class

        private void findAncestors()
        {
            assert pending.isEmpty();
            int nSegments = hKey.segments();
            for (int depth : ancestorTypeDepth) {
                hKey.useSegments(depth);
                Row row = readAncestorRow(hKey);
                if (row != null) {
                    pending.add(row);
                }
            }
            // Restore the hkey to its original state
            hKey.useSegments(nSegments);
        }

        private Row readAncestorRow(HKey hKey)
        {
            Row row;
            ancestorCursor.close();
            ancestorCursor.rebind(hKey, false);
            ancestorCursor.open();
            row = ancestorCursor.next();
            // Retrieved row might not actually be what we were looking for -- not all ancestors are present,
            // (there are orphan rows).
            if (row != null && !hKey.equals(row.hKey())) {
                row = null;
            }
            return row;
        }

        // Object state

        private final GroupCursor ancestorCursor;
        private final PendingRows pending;
        private final HKey hKey;
    }
}
