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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 <h1>Overview</h1>

 Select_HKeyOrdered passes on selected rows from the input stream to the output stream. A row is subject to elimination
 if and only if it's type is a specified type (predicateType), or a descendent of this type.
 
 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType predicateRowType:</b> Type of row to which the selection predicate is applied.
 <li><b>Expression predicate:</b> Selection predicate.
 
 <h1>Behavior</h1>
 
 The handling of a row depends on its RowType:
 
 If the row's type matches predicateRowType: The predicate is evaluated. The row is written to the output stream
 if and only if the predicate evaluates to true. 
 
 If the row's type is a descendent type of predicateRowType: The row is written to the output stream if and only if
 the predicate evaluated to true for the ancestor of type predicateRowType. (E.g., if a Customer is rejected,
 then all of its Orders and Items will be rejected too.)
 
 All other rows are written to the output stream unconditionally.

 <h1>Output</h1>

 A subset of the rows from the input stream.

 <h1>Assumptions</h1>

 Input is hkey-ordered with respect to predicateRowType. E.g., in a COI schema, with prediateRowType = Order, 
 Orders and Items are assumed to be in hkey-order. The order of one Order relative to another is not significant, nor
 is the order of Customers.

 <h1>Performance</h1>

 Project_Default does no IO. For each input row, the type is checked and each output field is computed.

 <h1>Memory Requirements</h1>

 One row of type predicateRowType.
 
 */

class Select_HKeyOrdered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", getClass().getSimpleName(), predicateRowType, predicate);
    }

    // Operator interface


    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Select_HKeyOrdered interface

    public Select_HKeyOrdered(Operator inputOperator, RowType predicateRowType, Expression predicate)
    {
        ArgumentValidation.notNull("predicateRowType", predicateRowType);
        ArgumentValidation.notNull("predicate", predicate);
        this.inputOperator = inputOperator;
        this.predicateRowType = predicateRowType;
        this.predicate = predicate;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered next");
    
    // Object state

    private final Operator inputOperator;
    private final RowType predicateRowType;
    private final Expression predicate;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                input.open();
                this.evaluation.of(context);
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                checkQueryCancelation();
                Row row = null;
                Row inputRow = input.next();
                while (row == null && inputRow != null) {
                    if (inputRow.rowType() == predicateRowType) {
                        evaluation.of(inputRow);
                        if (Extractors.getBooleanExtractor().getBoolean(evaluation.eval(), false)) {
                            // New row of predicateRowType
                            selectedRow.hold(inputRow);
                            row = inputRow;
                        }
                    } else if (predicateRowType.ancestorOf(inputRow.rowType())) {
                        // Row's type is a descendent of predicateRowType.
                        if (selectedRow.isHolding() && selectedRow.get().ancestorOf(inputRow)) {
                            row = inputRow;
                        } else {
                            selectedRow.release();
                        }
                    } else {
                        row = inputRow;
                    }
                    if (row == null) {
                        inputRow = input.next();
                    }
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            selectedRow.release();
            input.close();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
            this.evaluation = predicate.evaluation();
        }

        // Object state

        private final Cursor input;
        private final ShareHolder<Row> selectedRow = new ShareHolder<Row>(); // The last input row with type = predicateRowType.
        private final ExpressionEvaluation evaluation;
    }
}
