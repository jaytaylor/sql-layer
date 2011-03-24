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

import com.akiban.qp.Cursor;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;

public class Select_HKeyOrdered implements PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public Cursor cursor()
    {
        return new Execution();
    }

    // GroupScan_Default interface

    public Select_HKeyOrdered(PhysicalOperator inputOperator, RowType predicateRowType, Expression predicate)
    {
        this.inputOperator = inputOperator;
        this.predicateRowType = predicateRowType;
        this.predicate = predicate;
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType predicateRowType;
    private final Expression predicate;

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
            outputRow(null);
            boolean moreInput = input.next();
            while (outputRow() == null && moreInput) {
                if (input.rowType() == predicateRowType) {
                    // New row of predicateRowType
                    if ((Boolean) predicate.evaluate(input)) {
                        selectedRow.set(input.managedRow());
                        outputRow(selectedRow);
                    }
                } else if (predicateRowType.ancestorOf(input.rowType())) {
                    // Row's type is a descendent of predicateRowType.
                    if (selectedRow.isNotNull() && selectedRow.ancestorOf(input)) {
                        outputRow(input.managedRow());
                    } else {
                        selectedRow.set(null);
                    }
                } else {
                    outputRow(input.currentRow());
                }
                if (outputRow() == null) {
                    moreInput = input.next();
                }
            }
            return outputRow() != null;
        }

        @Override
        public void close()
        {
            selectedRow.set(null);
            input.close();
        }

        // Execution interface

        Execution()
        {
            input = inputOperator.cursor();
        }

        // Object state

        private final Cursor input;
        // row is the last input row with type = predicateRowType. For that row, rowSelected records the result
        // of predicate.evaluate(row).
        private final RowHolder<ManagedRow> selectedRow = new RowHolder<ManagedRow>();
    }
}
