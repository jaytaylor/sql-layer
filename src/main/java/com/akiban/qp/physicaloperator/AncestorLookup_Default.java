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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
        return String.format("%s(%s -> %s", getClass().getSimpleName(), rowType, ancestorTypes);
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
                                  List<RowType> ancestorTypes)
    {
        // Check arguments
        if (ancestorTypes.isEmpty()) {
            throw new IllegalArgumentException();
        }
        RowType tableRowType =
            rowType instanceof IndexRowType
            ? ((IndexRowType)rowType).tableType()
            : rowType;
        for (RowType ancestorType : ancestorTypes) {
            if (ancestorType == tableRowType) {
                throw new IllegalArgumentException(ancestorType.toString());
            }
            if (!ancestorType.ancestorOf(tableRowType)) {
                throw new IllegalArgumentException(ancestorType.toString());
            }
        }
        // Arguments are OK
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.rowType = rowType;
        // Handling of a group row is complete once it's been emitted (which happens later).
        // An index row is handled as soon as it's read because it will not be emitted.
        this.emitInputRow = !(rowType instanceof IndexRowType);
        // Sort ancestor types by depth
        this.ancestorTypes = new ArrayList<RowType>(ancestorTypes);
        Collections.sort(this.ancestorTypes,
                         new Comparator<RowType>()
                         {
                             @Override
                             public int compare(RowType x, RowType y)
                             {
                                 UserTable xTable = ((UserTableRowType) x).userTable();
                                 UserTable yTable = ((UserTableRowType) y).userTable();
                                 return xTable.getDepth() - yTable.getDepth();
                             }
                         });
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
    private final boolean emitInputRow;

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
        public boolean next()
        {
            while (pending.isEmpty() && inputRow.isNotNull()) {
                advance();
            }
            Row row = pending.take();
            outputRow(row);
            if (LOG.isInfoEnabled()) {
                LOG.info("Exhume: {}", row == null ? null : row);
            }
            return row != null;
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
            if (input.next()) {
                Row currentRow = input.currentRow();
                if (currentRow.rowType() == rowType) {
                    findAncestors(currentRow);
                }
                if (emitInputRow) {
                    pending.add(currentRow);
                }
                inputRow.set(currentRow);
            } else {
                inputRow.set(null);
            }
        }

        private void findAncestors(Row row)
        {
            assert pending.isEmpty();
            HKey hKey = row.hKey();
            int nSegments = hKey.segments();
            for (int i = 0; i < ancestorTypeDepth.length; i++) {
                int depth = ancestorTypeDepth[i];
                hKey.useSegments(depth);
                readAncestorRow(hKey);
                if (ancestorRow.isNotNull()) {
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
                if (ancestorCursor.next()) {
                    Row retrievedRow = ancestorCursor.currentRow();
                    // Retrieved row might not actually be what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    ancestorRow.set(hKey.equals(retrievedRow.hKey()) ? retrievedRow : null);
                } else {
                    ancestorRow.set(null);
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
