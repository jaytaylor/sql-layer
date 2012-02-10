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
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 If the input stream has at least one row, the output stream is identical to the input stream.
 Otherwise, the output stream contains one row, composed by a specified list of expressions.

 <h1>Arguments</h1>

 <ul>

 <li>Operator inputOperator:</li> Operator providing input stream.
 
 <li>RowType rowType:</li> Type of the row that is output in case the input stream is empty.
 
 <li>List<? extends Expression>:</li> Expressions computing the columns of the row that is output
 in case the input stream is empty.

 <ul>

 <h1>Behavior</h1>

 If the input stream has at least one row, the output stream is identical to the input stream.
 Otherwise, the output stream contains one row, composed by a specified list of expressions.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 This operator does not IO.

 <h1>Memory Requirements</h1>

 None.

 */

class IfEmpty_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName());
        buffer.append('(');
        boolean first = true;
        for (Expression expression : expressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(expression.toString());
        }
        buffer.append(')');
        return buffer.toString();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        ArrayList<Operator> inputOperators = new ArrayList<Operator>(1);
        inputOperators.add(inputOperator);
        return inputOperators;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Project_Default interface

    public IfEmpty_Default(Operator inputOperator,
                           RowType rowType,
                           List<? extends Expression> expressions)
    {
        ArgumentValidation.notNull("inputOperator", inputOperator);
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notNull("expressions", expressions);
        ArgumentValidation.isEQ("rowType.nFields()", rowType.nFields(), "expressions.size()", expressions.size());
        this.inputOperator = inputOperator;
        this.rowType = rowType;
        this.expressions = new ArrayList<Expression>(expressions);
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final Operator inputOperator;
    private final RowType rowType;
    private final List<Expression> expressions;

    // Inner classes

    enum InputState
    {
        UNKNOWN, EMPTY, NON_EMPTY
    }

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                this.input.open();
                this.closed = false;
                this.inputState = InputState.UNKNOWN;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                Row row = null;
                checkQueryCancelation();
                switch (inputState) {
                    case UNKNOWN:
                        row = input.next();
                        if (row == null) {
                            row = emptySubstitute();
                            inputState = InputState.EMPTY;
                        } else {
                            inputState = InputState.NON_EMPTY;
                        }
                        break;
                    case EMPTY:
                        row = null;
                        break;
                    case NON_EMPTY:
                        row = input.next();
                        break;
                }
                if (row == null) {
                    close();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("IfEmpty_Default: yield {}", row);
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            if (!closed) {
                input.close();
                closed = true;
            }
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.input = inputOperator.cursor(context);
            if (expressions == null) {
                this.evaluations = null;
            } else {
                this.evaluations = new ArrayList<ExpressionEvaluation>();
                for (Expression outerJoinRowExpression : expressions) {
                    ExpressionEvaluation eval = outerJoinRowExpression.evaluation();
                    evaluations.add(eval);
                }
            }
        }

        // For use by this class

        private Row emptySubstitute()
        {
            ValuesHolderRow valuesHolderRow = unsharedEmptySubstitute().get();
            int nFields = rowType.nFields();
            for (int i = 0; i < nFields; i++) {
                ExpressionEvaluation outerJoinRowColumnEvaluation = evaluations.get(i);
                outerJoinRowColumnEvaluation.of(context);
                valuesHolderRow.holderAt(i).copyFrom(outerJoinRowColumnEvaluation.eval());
            }
            return valuesHolderRow;
        }

        private ShareHolder<ValuesHolderRow> unsharedEmptySubstitute()
        {
            if (emptySubstitute.isEmpty() || emptySubstitute.isShared()) {
                ValuesHolderRow valuesHolderRow = new ValuesHolderRow(rowType);
                emptySubstitute.hold(valuesHolderRow);
            }
            return emptySubstitute;
        }

        // Object state

        private final Cursor input;
        private final List<ExpressionEvaluation> evaluations;
        private final ShareHolder<ValuesHolderRow> emptySubstitute = new ShareHolder<ValuesHolderRow>();
        private boolean closed;
        private InputState inputState;
    }
}
