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
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
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
    private static final PointTap ANC_LOOKUP_COUNT = Tap.createCount("operator: ancestor_lookup_nested", true);

    // Object state

    private final GroupTable groupTable;
    private final RowType rowType;
    private final List<RowType> ancestorTypes;
    private final int[] ancestorTypeDepth;
    private final int inputBindingPosition;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            ANC_LOOKUP_COUNT.hit();
            Row rowFromBindings = (Row) bindings.get(inputBindingPosition);
            if (LOG.isDebugEnabled()) {
                LOG.debug("AncestorLookup_Nested: open using {}", rowFromBindings);
            }
            assert rowFromBindings.rowType() == rowType : rowFromBindings;
            rowFromBindings.hKey().copyTo(hKey);
            findAncestors();
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row = pending.take();
            if (LOG.isDebugEnabled()) {
                LOG.debug("AncestorLookup: {}", row == null ? null : row);
            }
            if (row == null) {
                close();
            }
            return row;
        }

        @Override
        public void close()
        {
            pending.clear();
            ancestorCursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            super(adapter);
            this.pending = new PendingRows(ancestorTypeDepth.length);
            this.ancestorCursor = adapter.newGroupCursor(groupTable);
            this.hKey = adapter.newHKey(rowType);
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
            ancestorCursor.open(UndefBindings.only());
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
