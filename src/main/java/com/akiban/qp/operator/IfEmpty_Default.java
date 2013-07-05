/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
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
        List<?> toStringExpressions = (pExpressions != null) ? pExpressions : oExpressions;
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
    protected Cursor cursor(QueryContext context, QueryBindings bindings)
    {
        return new Execution(context, bindings);
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

    // Project_Default interface

    public IfEmpty_Default(Operator inputOperator,
                           RowType rowType,
                           List<? extends Expression> expressions,
                           List<? extends TPreparedExpression> pExpressions,
                           API.InputPreservationOption inputPreservation)
    {
        ArgumentValidation.notNull("inputOperator", inputOperator);
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notNull("inputPreservation", inputPreservation);
        this.inputOperator = inputOperator;
        this.rowType = rowType;
        List<?> validateExprs;
        if (pExpressions != null) {
            assert expressions == null : " expressions and pexpressions can't both be non-null";
            this.pExpressions = new ArrayList<>(pExpressions);
            this.oExpressions = null;
            validateExprs = pExpressions;
        }
        else if (expressions != null) {
            this.pExpressions = null;
            this.oExpressions = new ArrayList<>(expressions);
            validateExprs = expressions;
        }
        else {
            throw new IllegalArgumentException("both expressions lists are null");
        }
        ArgumentValidation.isEQ("rowType.nFields()", rowType.nFields(), "expressions.size()", validateExprs.size());
        this.inputPreservation = inputPreservation;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: IfEmpty_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final Operator inputOperator;
    private final RowType rowType;
    private final List<Expression> oExpressions;
    private final List<TPreparedExpression> pExpressions;
    private final API.InputPreservationOption inputPreservation;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INPUT_TYPE, rowType.getExplainer(context));
        if (pExpressions != null) {
            for (TPreparedExpression ex : pExpressions)
                atts.put(Label.OPERAND, ex.getExplainer(context));
        }
        else {
            for (Expression ex : oExpressions)
                atts.put(Label.OPERAND, ex.getExplainer(context));
        }
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        atts.put(Label.INPUT_PRESERVATION, PrimitiveExplainer.getInstance(inputPreservation.toString()));
        return new CompoundExplainer(Type.IF_EMPTY, atts);
    }

    // Inner classes

    enum InputState
    {
        UNKNOWN, DONE, ECHO_INPUT
    }

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
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
                    close();
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

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                input.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            input.destroy();
            if (oEvaluations != null) {
                for (ExpressionEvaluation evaluation : oEvaluations) {
                    evaluation.destroy();
                }
            }
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, QueryBindings bindings)
        {
            super(context, bindings);
            this.input = inputOperator.cursor(context, bindings);
            if (pExpressions != null) {
                this.oEvaluations = null;
                this.pEvaluations = new ArrayList<>(pExpressions.size());
                for (TPreparedExpression outerJoinRowExpressions : pExpressions) {
                    TEvaluatableExpression eval = outerJoinRowExpressions.build();
                    pEvaluations.add(eval);
                }
            } else {
                this.oEvaluations = new ArrayList<>();
                for (Expression outerJoinRowExpression : oExpressions) {
                    ExpressionEvaluation eval = outerJoinRowExpression.evaluation();
                    oEvaluations.add(eval);
                }
                this.pEvaluations = null;
            }
        }

        // For use by this class

        private Row emptySubstitute()
        {
            ValuesHolderRow valuesHolderRow = unsharedEmptySubstitute().get();
            int nFields = rowType.nFields();
            if (pEvaluations != null) {
                for (int i = 0; i < nFields; i++) {
                    TEvaluatableExpression outerJoinRowColumnEvaluation = pEvaluations.get(i);
                    outerJoinRowColumnEvaluation.with(context);
                    outerJoinRowColumnEvaluation.evaluate();
                    PValueTargets.copyFrom(
                            outerJoinRowColumnEvaluation.resultValue(),
                            valuesHolderRow.pvalueAt(i));
                }
            }
            else {
                for (int i = 0; i < nFields; i++) {
                    ExpressionEvaluation outerJoinRowColumnEvaluation = oEvaluations.get(i);
                    outerJoinRowColumnEvaluation.of(context);
                    valuesHolderRow.holderAt(i).copyFrom(outerJoinRowColumnEvaluation.eval());
                }
            }
            return valuesHolderRow;
        }

        private ShareHolder<ValuesHolderRow> unsharedEmptySubstitute()
        {
            if (emptySubstitute.isEmpty() || emptySubstitute.isShared()) {
                ValuesHolderRow valuesHolderRow = new ValuesHolderRow(rowType, pEvaluations != null);
                emptySubstitute.hold(valuesHolderRow);
            }
            return emptySubstitute;
        }

        // Object state

        private final Cursor input;
        private final List<ExpressionEvaluation> oEvaluations;
        private final List<TEvaluatableExpression> pEvaluations;
        private final ShareHolder<ValuesHolderRow> emptySubstitute = new ShareHolder<>();
        private boolean closed = true;
        private InputState inputState;
    }
}
