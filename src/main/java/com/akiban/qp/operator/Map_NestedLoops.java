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
import com.akiban.util.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Map_NestedLoops extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // Operator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        innerInputOperator.findDerivedTypes(derivedTypes);
        outerInputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(2);
        result.add(outerInputOperator);
        result.add(innerInputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(outerInputOperator), describePlan(innerInputOperator));
    }

    // Project_Default interface

    public Map_NestedLoops(Operator outerInputOperator,
                           Operator innerInputOperator,
                           RowType outerJoinRowType,
                           List<? extends Expression> outerJoinRowExpressions,
                           int inputBindingPosition)
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.isTrue("outer join specification makes sense",
                                  (outerJoinRowType == null && outerJoinRowExpressions == null) ||
                                  (outerJoinRowType != null &&
                                   outerJoinRowExpressions != null &&
                                   outerJoinRowExpressions.size() > 0));
        ArgumentValidation.isGTE("inputBindingPosition", inputBindingPosition, 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        this.outerJoinRowType = outerJoinRowType;
        this.outerJoinRowExpressions =
            outerJoinRowExpressions == null
            ? null
            : new ArrayList<Expression>(outerJoinRowExpressions);
        this.inputBindingPosition = inputBindingPosition;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);
    private static final Tap.PointTap MAP_NL_COUNT = Tap.createCount("operator: map_nested_loops", true);

    // Object state

    private final Operator outerInputOperator;
    private final Operator innerInputOperator;
    private final RowType outerJoinRowType;
    private final List<Expression> outerJoinRowExpressions;
    private final int inputBindingPosition;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
       	    MAP_NL_COUNT.hit();
            this.bindings = bindings;
            this.outerInput.open(bindings);
            this.closed = false;
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row outputRow = null;
            while (!closed && outputRow == null) {
                outputRow = nextOutputRow();
                if (outputRow == null) {
                    Row row = outerInput.next();
                    if (row == null) {
                        close();
                    } else {
                        outerRow.hold(row);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Map_NestedLoops: restart inner loop using current branch row");
                        }
                        startNewInnerLoop(row);
                    }
                }
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("Map_NestedLoops: yield {}", outputRow);
            }
            return outputRow;
        }

        @Override
        public void close()
        {
            if (!closed) {
                innerInput.close();
                closeOuter();
                closed = true;
            }
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            super(adapter);
            this.outerInput = outerInputOperator.cursor(adapter);
            this.innerInput = innerInputOperator.cursor(adapter);
            if (outerJoinRowExpressions == null) {
                this.outerJoinRowEvaluations = null;
            } else {
                this.outerJoinRowEvaluations = new ArrayList<ExpressionEvaluation>();
                for (Expression outerJoinRowExpression : outerJoinRowExpressions) {
                    ExpressionEvaluation eval = outerJoinRowExpression.evaluation();
                    eval.of(adapter);
                    outerJoinRowEvaluations.add(eval);
                }
            }
        }

        // For use by this class

        private Row nextOutputRow()
        {
            Row outputRow = null;
            if (outerRow.isHolding()) {
                Row innerRow = innerInput.next();
                if (innerRow == null) {
                    if (needOuterJoinRow) {
                        ValuesHolderRow outerJoinValuesHolderRow = unsharedOuterJoinRow().get();
                        int nFields = outerJoinRowType.nFields();
                        for (int i = 0; i < nFields; i++) {
                            ExpressionEvaluation outerJoinRowColumnEvaluation = outerJoinRowEvaluations.get(i);
                            outerJoinRowColumnEvaluation.of(outerRow.get());
                            outerJoinRowColumnEvaluation.of(bindings);
                            outerJoinValuesHolderRow.holderAt(i).copyFrom(outerJoinRowColumnEvaluation.eval());
                        }
                        outputRow = outerJoinValuesHolderRow;
                        // We're about to emit an outerjoin row. Don't do it again for this outer row.
                        needOuterJoinRow = false;
                    } else {
                        outerRow.release();
                    }
                } else {
                    outputRow = innerRow;
                    needOuterJoinRow = false;
                }
            }
            return outputRow;
        }

        private void closeOuter()
        {
            outerRow.release();
            outerJoinRow.release();
            outerInput.close();
        }

        private void startNewInnerLoop(Row row)
        {
            innerInput.close();
            bindings.set(inputBindingPosition, row);
            innerInput.open(bindings);
            needOuterJoinRow = outerJoinRowType != null;
        }

        private ShareHolder<ValuesHolderRow> unsharedOuterJoinRow()
        {
            if (outerJoinRow.isEmpty() || outerJoinRow.isShared()) {
                ValuesHolderRow valuesHolderRow = new ValuesHolderRow(outerJoinRowType);
                outerJoinRow.hold(valuesHolderRow);
            }
            return outerJoinRow;
        }

        // Object state

        private final Cursor outerInput;
        private final Cursor innerInput;
        private final ShareHolder<Row> outerRow = new ShareHolder<Row>();
        private final List<ExpressionEvaluation> outerJoinRowEvaluations;
        private final ShareHolder<ValuesHolderRow> outerJoinRow = new ShareHolder<ValuesHolderRow>();
        private Bindings bindings;
        private boolean closed = false;
        private boolean needOuterJoinRow;
    }
}
