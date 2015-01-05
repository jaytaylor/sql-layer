/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 If the input stream has no rows, the output stream contains one row, composed by a specified list of expressions.
 Otherwise, the output is either the input rows or no rows at all, controlled by an InputPreservationOption.

 <h1>Arguments</h1>

 <ul>

 <li>Operator inputOperator:</li> Operator providing input stream.
 
 <li>RowType rowType:</li> Type of the row that is output in case the input stream is empty.
 
 <li>List<? extends Expression>:</li> Expressions computing the columns of the row that is output
 in case the input stream is empty.

 <li>InputPreservationOption inputPreservation:</li> indicates whether input rows are output when present.

 <ul>

 <h1>Behavior</h1>

 If the input stream has no rows, then a row, composed by a specified list of expressions, is written to the output
 stream. Otherwise, input rows are written to output if inputPreservation is KEEP_INPUT; otherwise input rows
 are not written to output.

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
        List<?> toStringExpressions = pExpressions;
        for (Object expression : toStringExpressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(expression.toString());
        }
        buffer.append(", ");
        buffer.append(inputPreservation);
        buffer.append(')');
        return buffer.toString();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        ArrayList<Operator> inputOperators = new ArrayList<>(1);
        inputOperators.add(inputOperator);
        return inputOperators;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // IfEmpty_Default interface

    public IfEmpty_Default(Operator inputOperator,
                           RowType rowType,
                           List<? extends TPreparedExpression> pExpressions,
                           API.InputPreservationOption inputPreservation)
    {
        ArgumentValidation.notNull("inputOperator", inputOperator);
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notNull("inputPreservation", inputPreservation);
        ArgumentValidation.notNull("pExpressions", pExpressions);
        this.inputOperator = inputOperator;
        this.rowType = rowType;
        List<?> validateExprs = pExpressions;
        this.pExpressions = new ArrayList<>(pExpressions);
        ArgumentValidation.isEQ("rowType.nFields()", rowType.nFields(), "expressions.size()", validateExprs.size());
        this.inputPreservation = inputPreservation;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(IfEmpty_Default.class);

    // Object state

    private final Operator inputOperator;
    private final RowType rowType;
    private final List<TPreparedExpression> pExpressions;
    private final API.InputPreservationOption inputPreservation;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INPUT_TYPE, rowType.getExplainer(context));
        for (TPreparedExpression ex : pExpressions)
            atts.put(Label.OPERAND, ex.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        atts.put(Label.INPUT_PRESERVATION, PrimitiveExplainer.getInstance(inputPreservation.toString()));
        return new CompoundExplainer(Type.IF_EMPTY, atts);
    }

    // Inner classes

    enum InputState
    {
        UNKNOWN, DONE, ECHO_INPUT
    }

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                this.inputState = InputState.UNKNOWN;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row row = null;
                checkQueryCancelation();
                switch (inputState) {
                    case UNKNOWN:
                        row = input.next();
                        if (row == null) {
                            row = emptySubstitute();
                            inputState = InputState.DONE;
                        } else if (inputPreservation == API.InputPreservationOption.KEEP_INPUT) {
                            inputState = InputState.ECHO_INPUT;
                        } else {
                            row = null;
                            inputState = InputState.DONE;
                        }
                        break;
                    case DONE:
                        row = null;
                        break;
                    case ECHO_INPUT:
                        row = input.next();
                        break;
                }
                if (row == null) {
                    setIdle();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("IfEmpty_Default: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

         // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, inputOperator.cursor(context, bindingsCursor));
            this.pEvaluations = new ArrayList<>(pExpressions.size());
            for (TPreparedExpression outerJoinRowExpressions : pExpressions) {
                TEvaluatableExpression eval = outerJoinRowExpressions.build();
                pEvaluations.add(eval);
            }
        }

        // For use by this class

        private Row emptySubstitute()
        {
            ValuesHolderRow valuesHolderRow = new ValuesHolderRow(rowType);
            int nFields = rowType.nFields();
            for (int i = 0; i < nFields; i++) {
                TEvaluatableExpression outerJoinRowColumnEvaluation = pEvaluations.get(i);
                outerJoinRowColumnEvaluation.with(context);
                outerJoinRowColumnEvaluation.with(bindings);
                outerJoinRowColumnEvaluation.evaluate();
                ValueTargets.copyFrom(
                        outerJoinRowColumnEvaluation.resultValue(),
                        valuesHolderRow.valueAt(i));
            }
            return valuesHolderRow;
        }

        // Object state

        private final List<TEvaluatableExpression> pEvaluations;
        private InputState inputState;
    }
}
