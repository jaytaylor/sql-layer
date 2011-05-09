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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;

class Select_HKeyOrdered extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", getClass().getSimpleName(), predicateRowType, predicate);
    }

    // PhysicalOperator interface

    @Override
    public Cursor cursor(StoreAdapter adapter, Bindings bindings)
    {
        return new Execution(adapter, inputOperator.cursor(adapter, bindings));
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
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public boolean next()
        {
            outputRow(null);
            boolean moreInput = input.next();
            while (outputRow() == null && moreInput) {
                Row inputRow = input.currentRow();
                if (inputRow.rowType() == predicateRowType) {
                    // New row of predicateRowType
                    if ((Boolean) predicate.evaluate(inputRow)) {
                        selectedRow.set(inputRow);
                        outputRow(selectedRow.get());
                    }
                } else if (predicateRowType.ancestorOf(inputRow.rowType())) {
                    // Row's type is a descendent of predicateRowType.
                    if (selectedRow.isNotNull() && selectedRow.get().ancestorOf(inputRow)) {
                        outputRow(inputRow);
                    } else {
                        selectedRow.set(null);
                    }
                } else {
                    outputRow(inputRow);
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

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.input = input;
        }

        // Object state

        private final Cursor input;
        // selectedRow is the last input row with type = predicateRowType.
        private final RowHolder<Row> selectedRow = new RowHolder<Row>();
    }
}
