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

package com.akiban.qp;

public class Select_HKeyOrdered implements PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public void open()
    {
        input.open();
    }

    @Override
    public Row next()
    {
        Row outputRow = null;
        Row inputRow = input.next();
        while (outputRow == null && inputRow != null) {
            if (inputRow.type() == predicateRowType) {
                // New row of predicateRowType
                if ((Boolean) predicate.evaluate(inputRow)) {
                    row = inputRow.copy();
                    outputRow = row;
                }
            } else if (predicateRowType.ancestorOf(inputRow.type())) {
                // Row's type is a descendent of predicateRowType.
                if (row != null && row.ancestorOf(inputRow)) {
                    outputRow = inputRow;
                } else {
                    row = null;
                }
            } else {
                outputRow = inputRow;
            }
            if (outputRow == null) {
                inputRow = input.next();
            }
        }
        return outputRow;
    }

    @Override
    public void close()
    {
        input.close();
    }

    // GroupScan_Default interface

    public Select_HKeyOrdered(Cursor input, RowType predicateRowType, Expression predicate)
    {
        this.input = input;
        this.predicateRowType = predicateRowType;
        this.predicate = predicate;
    }

    // Object state

    private final Cursor input;
    private final RowType predicateRowType;
    private final Expression predicate;
    // row is the last input row with type = predicateRowType. For that row, rowSelected records the result
    // of predicate.evaluate(row).
    private Row row;
}
