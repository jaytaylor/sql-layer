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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.max;

class AncestorLookup_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), rowType, ancestorTypes);
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    @Override
    public List<PhysicalOperator> getInputOperators()
    {
        List<PhysicalOperator> result = new ArrayList<PhysicalOperator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // AncestorLookup_Default interface

    public AncestorLookup_Default(PhysicalOperator inputOperator,
                                  GroupTable groupTable,
                                  RowType rowType,
                                  Collection<? extends RowType> ancestorTypes,
                                  boolean keepInput)
    {
        ArgumentValidation.notEmpty("ancestorTypes", ancestorTypes);
        // Keeping index rows not currently supported
        boolean inputFromIndex = rowType instanceof IndexRowType;
        ArgumentValidation.isTrue("!(keepInput && inputFromIndex)", !(keepInput && inputFromIndex));
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
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.rowType = rowType;
        this.keepInput = keepInput;
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
            UserTable userTable = ((UserTableRowType) ancestorType).userTable();
            this.ancestorTypeDepth[a++] = userTable.getDepth() + 1;
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(AncestorLookup_Default.class);

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final RowType rowType;
    private final List<RowType> ancestorTypes;
    private final int[] ancestorTypeDepth;
    private final boolean keepInput;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
            advance();
        }

        @Override
        public boolean booleanNext()
        {
            assert false;
            return false;
        }

        @Override
        public Row next()
        {
            while (pending.isEmpty() && inputRow.isNotNull()) {
                advance();
            }
            Row row = pending.take();
            if (LOG.isDebugEnabled()) {
                LOG.debug("AncestorLookup: {}", row == null ? null : row);
            }
            return row;
        }

        @Override
        public void close()
        {
            outputRow(null);
            input.close();
            ancestorRow.set(null);
            pending.clear();
        }

        // For use by this class

        private void advance()
        {
            Row currentRow = input.next();
            if (currentRow != null) {
                if (currentRow.rowType() == rowType) {
                    findAncestors(currentRow);
                }
                if (keepInput) {
                    pending.add(currentRow);
                }
                inputRow.set(currentRow);
            } else {
                inputRow.set(null);
            }
        }

        private void findAncestors(Row inputRow)
        {
            assert pending.isEmpty();
            HKey hKey = inputRow.hKey();
            int nSegments = hKey.segments();
            for (int i = 0; i < ancestorTypeDepth.length; i++) {
                int depth = ancestorTypeDepth[i];
                hKey.useSegments(depth);
                readAncestorRow(hKey);
                if (ancestorRow.isNotNull()) {
                    ancestorRow.get().runId(inputRow.runId());
                    pending.add(ancestorRow.get());
                }
            }
            // Restore the hkey to its original state
            hKey.useSegments(nSegments);
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.input = input;
            // Why + 1: Because the input row (whose ancestors get discovered) also goes into pending.
            this.pending = new PendingRows(ancestorTypeDepth.length + 1);
            this.ancestorCursor = adapter.newGroupCursor(groupTable);
        }

        // For use by this class

        private void readAncestorRow(HKey hKey)
        {
            try {
                ancestorCursor.rebind(hKey, false);
                ancestorCursor.open(UndefBindings.only());
                Row retrievedRow = ancestorCursor.next();
                if (retrievedRow == null) {
                    ancestorRow.set(null);
                } else {
                    // Retrieved row might not actually be what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    ancestorRow.set(hKey.equals(retrievedRow.hKey()) ? retrievedRow : null);
                }
            } finally {
                ancestorCursor.close();
            }
        }

        // Object state

        private final Cursor input;
        private final RowHolder<Row> inputRow = new RowHolder<Row>();
        private final GroupCursor ancestorCursor;
        private final RowHolder<Row> ancestorRow = new RowHolder<Row>();
        private final PendingRows pending;
    }
}
