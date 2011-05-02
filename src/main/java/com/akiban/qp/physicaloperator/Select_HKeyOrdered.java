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
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class Select_HKeyOrdered extends PhysicalOperator
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
        return String.format("%s(%s, %s)", getClass().getSimpleName(), predicateRowType, predicate);
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
                ManagedRow inputRow = input.currentRow();
                if (inputRow.rowType() == predicateRowType) {
                    // New row of predicateRowType
                    if ((Boolean) predicate.evaluate(inputRow)) {
                        selectedRow.set(inputRow);
                        outputRow(selectedRow.managedRow());
                    }
                } else if (predicateRowType.ancestorOf(inputRow.rowType())) {
                    // Row's type is a descendent of predicateRowType.
                    if (selectedRow.isNotNull() && selectedRow.managedRow().ancestorOf(inputRow)) {
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

        Execution(StoreAdapter adapter, OperatorExecution input)
        {
            super(adapter);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        // selectedRow is the last input row with type = predicateRowType.
        private final RowHolder<ManagedRow> selectedRow = new RowHolder<ManagedRow>();
    }
}
