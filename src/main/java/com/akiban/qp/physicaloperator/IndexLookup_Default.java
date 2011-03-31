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
import com.akiban.qp.*;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.List;

import static java.lang.Math.max;

public class IndexLookup_Default implements PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public Cursor cursor(BTreeAdapter adapter)
    {
        return new Execution(adapter);
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
            ManagedRow groupRow = null;
            while (groupRow == null && indexRow.isNotNull()) {
                groupRow = pending.take();
                if (groupRow == null) {
                    if (groupCursor.next()) {
                        groupRow = groupCursor.managedRow();
                        if (groupRow != null && !indexRow.ancestorOf(groupRow)) {
                            groupRow = null;
                        }
                    } else {
                        advanceIndex();
                    }
                }
            }
            outputRow(groupRow);
            return groupRow != null;
        }

        @Override
        public void close()
        {
            outputRow(null);
            indexInput.close();
        }

        // For use by this class

        private void advanceIndex()
        {
            groupCursor.close();
            if (indexInput.next()) {
                indexRow.set(indexInput.managedRow());
                groupCursor.open(indexRow.hKey());
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
                ManagedRow ancestor = readAncestorRow();
                if (ancestor != null) {
                    pending.add(ancestor);
                }
            }
            // Restore the hkey to its original state
            hKey.useSegments(nSegments);
        }

        // Execution interface

        Execution(BTreeAdapter adapter)
        {
            super(adapter);
            this.indexInput = (IndexCursor) inputOperator.cursor(adapter);
            this.groupCursor = adapter.newGroupCursor(groupTable);
            this.ancestorCursor = adapter.newGroupCursor(groupTable);
            this.pending = new PendingRows(missingTypeDepth.length);
        }

        // For use by this class

        private ManagedRow readAncestorRow()
        {
            ManagedRow row = null;
            try {
                ancestorCursor.open(indexRow.hKey());
                if (ancestorCursor.next()) {
                    row = ancestorCursor.managedRow();
                }
            } finally {
                ancestorCursor.close();
            }
            return row;
        }

        // Object state

        private final IndexCursor indexInput;
        private final RowHolder<ManagedRow> indexRow = new RowHolder<ManagedRow>();
        private final GroupCursor groupCursor;
        private final GroupCursor ancestorCursor;
        private final PendingRows pending;
    }
}
