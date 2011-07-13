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

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexScan_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s %s%s)", getClass().getSimpleName(), index, indexKeyRange, reverse ? " reverse" : "");
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
    }

    // IndexScan_Default interface

    public IndexScan_Default(IndexRowType indexType,
                             boolean reverse,
                             IndexKeyRange indexKeyRange,
                             UserTableRowType rootmostExistingRowType)
    {
        ArgumentValidation.notNull("indexType", indexType);
        this.index = indexType.index();
        this.reverse = reverse;
        this.indexKeyRange = indexKeyRange;
        if (index.isTableIndex()) {
            ArgumentValidation.isEQ(
                    "group index table", this.index.leafMostTable(),
                    "rootmost existing row type", rootmostExistingRowType.userTable()
            );
        }
        else {
            GroupIndex tableIndex = (GroupIndex)this.index;
            boolean rootmostRowTypeInSegment = false;
            for (
                    UserTable branchTable=tableIndex.leafMostTable();
                    branchTable!= null && !branchTable.equals(tableIndex.rootMostTable());
                    branchTable = branchTable.parentTable()
            ) {
                if (branchTable.equals(rootmostExistingRowType.userTable())) {
                    rootmostRowTypeInSegment = true;
                    break;
                }
            }
            if (!rootmostRowTypeInSegment) {
                throw new IllegalArgumentException(rootmostExistingRowType + " not in branch for " + tableIndex);
            }
        }
        this.rootmostExistingDepth = rootmostExistingRowType.userTable().getDepth();
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);

    // Object state

    private final Index index;
    private final boolean reverse;
    private final IndexKeyRange indexKeyRange;
    private final long rootmostExistingDepth;

    // Inner classes

    private class Execution implements Cursor
    {
        // OperatorExecution interface

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
        }

        @Override
        public Row next()
        {
            Row row = cursor.next();
            if (row == null) {
                close();
            } else {
                row.runId(runIdCounter++);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("IndexScan: {}", row);
            }
            return row;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            this.cursor = adapter.newIndexCursor(index, reverse, indexKeyRange);
        }

        // Object state

        private final Cursor cursor;
        private int runIdCounter = 0;
    }
}
