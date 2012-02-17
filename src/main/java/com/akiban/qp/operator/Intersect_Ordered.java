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

import com.akiban.qp.row.IntersectRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.IntersectRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.akiban.qp.operator.API.JoinType;

class Intersect_Ordered extends Operator
{
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        right.findDerivedTypes(derivedTypes);
        left.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(2);
        result.add(left);
        result.add(right);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(left), describePlan(right));
    }

    // Project_Default interface

    public Intersect_Ordered(Operator left,
                             Operator right,
                             IndexRowType leftRowType,
                             IndexRowType rightRowType,
                             int orderingFields, // Assumed to be a suffix of leftRowType rows and rightRowType rows
                             JoinType joinType,
                             // TODO: Might want to get these positions in some other way. But QueryContext is too late.
                             int leftRowPosition, 
                             int rightRowPosition)
    {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftRowType);
        ArgumentValidation.notNull("rightRowType", rightRowType);
        ArgumentValidation.notNull("joinType", joinType);
        // TODO: Drop this requirement
        ArgumentValidation.isEQ("joinType", joinType, "JoinType.INNER_JOIN", JoinType.INNER_JOIN);
        this.left = left;
        this.right = right;
        this.leftRowType = leftRowType;
        this.rightRowType = rightRowType;
        this.orderingFields = orderingFields;
        this.joinType = joinType;
        this.leftRowPosition = leftRowPosition;
        this.rightRowPosition = rightRowPosition;
        this.outputRowType = leftRowType.schema().newIntersectType(leftRowType, rightRowType);
        // Setup for row comparisons
        fieldRankingExpressions = new RankExpression[orderingFields];
        int leftField = leftRowType.nFields() - orderingFields;
        int rightField = rightRowType.nFields() - orderingFields;
        for (int f = 0; f < orderingFields; f++) {
            fieldRankingExpressions[f] =
                new RankExpression(
                    new BoundFieldExpression(leftRowPosition, new FieldExpression(leftRowType, leftField)),
                    new BoundFieldExpression(rightRowPosition, new FieldExpression(rightRowType, rightField)));
            leftField++;
            rightField++;
        }
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private final IndexRowType leftRowType;
    private final IndexRowType rightRowType;
    private final int orderingFields;
    private final IntersectRowType outputRowType;
    private final JoinType joinType;
    private final int leftRowPosition;
    private final int rightRowPosition;
    private final RankExpression[] fieldRankingExpressions;
    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                leftInput.open();
                rightInput.open();
                nextLeftRow();
                nextRightRow();
                if (leftRow.isEmpty() && rightRow.isEmpty()) {
                    close();
                }
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                Row next = null;
                while (!closed && next == null) {
                    long c = compareRows();
                    if (c < 0) {
                        nextLeftRow();
                    } else if (c > 0) {
                        nextRightRow();
                    } else {
                        next = combineRows();
                    }
                    if (leftRow.isEmpty() && rightRow.isEmpty()) {
                        close();
                    }
                }
                return next;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            if (!closed) {
                leftRow.release();
                rightRow.release();
                leftInput.close();
                rightInput.close();
                closed = true;
            }
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            leftInput = left.cursor(context);
            rightInput = right.cursor(context);
            fieldRankingEvaluations = new ExpressionEvaluation[fieldRankingExpressions.length];
            for (int f = 0; f < fieldRankingEvaluations.length; f++) {
                fieldRankingEvaluations[f] = fieldRankingExpressions[f].evaluation();
                fieldRankingEvaluations[f].of(context);
            }
        }
        
        // For use by this class
        
        private void nextLeftRow()
        {
            Row row = leftInput.next();
            leftRow.hold(row);
            context.setRow(leftRowPosition, row);
        }
        
        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow.hold(row);
            context.setRow(rightRowPosition, row);
        }
        
        private long compareRows()
        {
            long c = 0;
            assert !closed;
            assert !(leftRow.isEmpty() && rightRow.isEmpty());
            if (leftRow.isEmpty()) {
                c = 1;
            } else if (rightRow.isEmpty()) {
                c = -1;
            } else {
                for (ExpressionEvaluation fieldRankingEvaluation : fieldRankingEvaluations) {
                    c = fieldRankingEvaluation.eval().getInt();
                    if (c != 0) {
                        break;
                    }
                }
            }
            return c;
        }
        
        private Row combineRows()
        {
            return new IntersectRow(outputRowType, leftRow.get(), rightRow.get());
        }

        // Object state
        
        // Rows from each input stream are bound to the QueryContext. However, QueryContext doesn't use
        // ShareHolders, so they are needed here.

        private boolean closed = false;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private final ShareHolder<Row> leftRow = new ShareHolder<Row>();
        private final ShareHolder<Row> rightRow = new ShareHolder<Row>();
        private final ExpressionEvaluation[] fieldRankingEvaluations;
    }
}
