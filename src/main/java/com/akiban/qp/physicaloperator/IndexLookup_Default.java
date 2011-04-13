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
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;

class IndexLookup_Default extends PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public OperatorExecution instantiate(StoreAdapter adapter, OperatorExecution[] ops)
    {
        ops[operatorId] = new Execution(adapter, inputOperator.instantiate(adapter, ops));
        return ops[operatorId];
    }

    @Override
    public void assignOperatorIds(AtomicInteger idGenerator)
    {
        inputOperator.assignOperatorIds(idGenerator);
        super.assignOperatorIds(idGenerator);
    }

    // IndexLookup_Default interface

    public IndexLookup_Default(PhysicalOperator inputOperator, GroupTable groupTable, List<RowType> missingTypes)
    {
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        int maxTypeId = -1;
        for (RowType missingType : missingTypes) {
            maxTypeId = max(maxTypeId, missingType.typeId());
        }
        this.missingTypeDepth = new int[maxTypeId + 1];
        for (RowType missingType : missingTypes) {
            UserTable userTable = ((UserTableRowType) missingType).userTable();
            this.missingTypeDepth[missingType.typeId()] = userTable.getDepth() + 1;
        }
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final int[] missingTypeDepth;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            indexInput.open();
            advanceIndex();
        }

        @Override
        public boolean next()
        {
            groupRow.set(null);
            while (groupRow.isNull() && indexRow.isNotNull()) {
                groupRow.set(pending.take());
                if (groupRow.isNull()) {
                    if (groupCursor.next()) {
                        groupRow.set(groupCursor.currentRow());
                        if (groupRow.isNotNull() && !indexRow.ancestorOf(groupRow.managedRow())) {
                            groupRow.set(null);
                        }
                    } else {
                        advanceIndex();
                    }
                }
            }
            outputRow(groupRow.managedRow());
            return groupRow.isNotNull();
        }

        @Override
        public void close()
        {
            outputRow(null);
            ancestorRow.set(null);
            indexInput.close();
        }

        // For use by this class

        private void advanceIndex()
        {
            groupCursor.close();
            if (indexInput.next()) {
                indexRow.set(indexInput.currentRow());
                groupCursor.bind(indexRow.hKey());
                groupCursor.open();
                findAncestors();
            } else {
                indexRow.set(null);
            }
        }

        private void findAncestors()
        {
            HKey hKey = indexRow.hKey();
            int nSegments = hKey.segments();
            for (int i = 1; i < missingTypeDepth.length; i++) {
                int depth = missingTypeDepth[i];
                hKey.useSegments(depth);
                readAncestorRow();
                if (ancestorRow.isNotNull()) {
                    pending.add(ancestorRow.managedRow());
                }
            }
            // Restore the hkey to its original state
            hKey.useSegments(nSegments);
        }

        // Execution interface

        Execution(StoreAdapter adapter, OperatorExecution input)
        {
            super(adapter);
            this.indexInput = input;
            this.groupCursor = adapter.newGroupCursor(groupTable);
            this.ancestorCursor = adapter.newGroupCursor(groupTable);
            this.pending = new PendingRows(missingTypeDepth.length);
        }

        // For use by this class

        private void readAncestorRow()
        {
            try {
                ancestorCursor.bind(indexRow.hKey());
                ancestorCursor.open();
                if (ancestorCursor.next()) {
                    ancestorRow.set(ancestorCursor.currentRow());
                }
            } finally {
                ancestorCursor.close();
            }
        }

        // Object state

        private final Cursor indexInput;
        private final RowHolder<ManagedRow> indexRow = new RowHolder<ManagedRow>();
        private final GroupCursor groupCursor;
        private final RowHolder<ManagedRow> groupRow = new RowHolder<ManagedRow>();
        private final GroupCursor ancestorCursor;
        private final RowHolder<ManagedRow> ancestorRow = new RowHolder<ManagedRow>();
        private final PendingRows pending;
    }
}
