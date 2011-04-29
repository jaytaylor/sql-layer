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

class Exhume_Default extends PhysicalOperator
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
        return String.format("%s(%s -> %s", getClass().getSimpleName(), rowType, ancestorTypes);
    }

    // Exhume_Default interface

    public Exhume_Default(PhysicalOperator inputOperator,
                          GroupTable groupTable,
                          RowType rowType,
                          List<RowType> ancestorTypes)
    {
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.rowType = rowType;
        this.ancestorTypes = ancestorTypes;
        int maxTypeId = -1;
        for (RowType missingType : ancestorTypes) {
            maxTypeId = max(maxTypeId, missingType.typeId());
        }
        this.ancestorTypeDepth = new int[maxTypeId + 1];
        for (RowType missingType : ancestorTypes) {
            UserTable userTable = ((UserTableRowType) missingType).userTable();
            this.ancestorTypeDepth[missingType.typeId()] = userTable.getDepth() + 1;
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(Exhume_Default.class);

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final RowType rowType;
    private final List<RowType> ancestorTypes;
    private final int[] ancestorTypeDepth;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            input.open();
        }

        @Override
        public boolean next()
        {
            if (pending.isEmpty()) {
                advance();
            }
            ManagedRow row = pending.take();
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
            ancestorCursor.close();
            ancestorRow.set(null);
            pending.clear();
        }

        // For use by this class

        private void advance()
        {
            if (input.next()) {
                ManagedRow currentRow = input.currentRow();
                if (currentRow.rowType() == rowType) {
                    findAncestors(currentRow);
                }
                pending.add(currentRow);
            }
        }

        private void findAncestors(ManagedRow row)
        {
            assert pending.isEmpty();
            HKey hKey = row.hKey();
            int nSegments = hKey.segments();
            for (int i = 1; i < ancestorTypeDepth.length; i++) {
                if (ancestorTypeDepth[i] > 0) {
                    int depth = ancestorTypeDepth[i];
                    hKey.useSegments(depth);
                    readAncestorRow(hKey);
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
            this.input = input;
            this.ancestorCursor = adapter.newGroupCursor(groupTable);
            // Why + 1: Because the input row (whose ancestors get discovered) also goes into pending.
            this.pending = new PendingRows(ancestorTypeDepth.length + 1);
        }

        // For use by this class

        private void readAncestorRow(HKey hKey)
        {
            try {
                ancestorCursor.bind(hKey);
                ancestorCursor.open();
                if (ancestorCursor.next()) {
                    ManagedRow retrievedRow = ancestorCursor.currentRow();
                    // Retrieved row might not actually what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    ancestorRow.set(hKey.equals(retrievedRow.hKey()) ? retrievedRow : null);
                }
            } finally {
                ancestorCursor.close();
            }
        }

        // Object state

        private final Cursor input;
        private final GroupCursor ancestorCursor;
        private final RowHolder<ManagedRow> ancestorRow = new RowHolder<ManagedRow>();
        private final PendingRows pending;
    }
}
