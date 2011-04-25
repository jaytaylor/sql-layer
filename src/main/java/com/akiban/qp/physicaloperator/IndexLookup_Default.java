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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    @Override
    public List<PhysicalOperator> getInputOperators() 
    {
        List<PhysicalOperator> result = new ArrayList<PhysicalOperator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String toString() 
    {
        return getClass().getSimpleName() + "(" + groupTable + " " + limit + ")";
    }

    // IndexLookup_Default interface

    public IndexLookup_Default(PhysicalOperator inputOperator,
                               GroupTable groupTable,
                               Limit limit,
                               List<RowType> missingTypes)
    {
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.limit = limit;
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

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexLookup_Default.class);

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final Limit limit;
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
            while (groupRow.isNull() && indexRow.isNotNull()) {
                groupRow.set(pending.take());
                if (groupRow.isNull()) {
                    if (groupCursor.next()) {
                        // Get descendent of row selected by index
                        groupRow.set(groupCursor.currentRow());
                    } else {
                        advanceIndex();
                    }
                }
            }
            outputRow(groupRow.managedRow());
            groupRow.set(null);
            if (LOG.isInfoEnabled()) {
                LOG.info("IndexLookup: {}", groupRow.isNull() ? null : groupRow.managedRow());
            }
            return outputRow() != null;
        }

        @Override
        public void close()
        {
            outputRow(null);
            indexInput.close();
            indexRow.set(null);
            groupCursor.close();
            groupRow.set(null);
            ancestorCursor.close();
            ancestorRow.set(null);
            pending.clear();
        }

        // For use by this class

        private void advanceIndex()
        {
            groupCursor.close();
            if (indexInput.next()) {
                indexRow.set(indexInput.currentRow());
                groupCursor.bind(this.indexRow.hKey());
                groupCursor.open();
                if (groupCursor.next()) {
                    ManagedRow groupRow = groupCursor.currentRow();
                    if (limit.limitReached(groupRow)) {
                        close();
                    } else {
                        findAncestors();
                        pending.add(groupRow);
                    }
                }
                groupRow.set(pending.take());
            } else {
                indexRow.set(null);
            }
        }

        private void findAncestors()
        {
            assert pending.isEmpty();
            HKey hKey = indexRow.hKey();
            int nSegments = hKey.segments();
            for (int i = 1; i < missingTypeDepth.length; i++) {
                if (missingTypeDepth[i] > 0) {
                    int depth = missingTypeDepth[i];
                    hKey.useSegments(depth);
                    readAncestorRow();
                    if (ancestorRow.isNotNull()) {
                        pending.add(ancestorRow.managedRow());
                    }
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
            // Why + 1: Because the group row (whose ancestors get discovered) also goes into pending.
            this.pending = new PendingRows(missingTypeDepth.length + 1);
        }

        // For use by this class

        private void readAncestorRow()
        {
            try {
                ancestorCursor.bind(indexRow.hKey());
                ancestorCursor.open();
                if (ancestorCursor.next()) {
                    ManagedRow retrievedRow = ancestorCursor.currentRow();
                    // Retrieved row might not actually what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    ancestorRow.set(indexRow.hKey().equals(retrievedRow.hKey()) ? retrievedRow : null);
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
