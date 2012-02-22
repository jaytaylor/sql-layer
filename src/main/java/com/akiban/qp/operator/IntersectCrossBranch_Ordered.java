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
import com.akiban.qp.rowtype.IntersectInBranchRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.akiban.qp.operator.API.JoinType;
import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 IntersectInBranch_Ordered finds pairs of rows from two input streams whose projection onto a set of common fields matches.
 Each input stream must be ordered by these common fields.
 For each pair, an output row of IntersectInBranchRowType, comprising both input rows, is generated.
 <p>Example: Suppose the left input is an index on (x, pid, cid) and the right input is an index on (a, b, pid, cid).
 The left index scan has restricted the value of x, and the right index scan has restricted the values of a and b.
 Both streams are therefore ordered by (pid, cid).

 <h1>Arguments</h1>

<li><b>Operator left:</b> Operator providing left input stream.
<li><b>Operator right:</b> Operator providing right input stream.
<li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
<li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
<li><b>int orderingFields:</b> Number of common fields, defining ordering and used for matching rows.
<li><b>JoinType joinType:</b>
   <ul>
     <li>INNER_JOIN: An ordinary intersection is computed.
     <li>LEFT_JOIN: Keep an unmatched row from the left input stream, filling out the row with nulls
     <li>RIGHT_JOIN: Keep an unmatched row from the right input stream, filling out the row with nulls
     <li>FULL_JOIN: Keep an unmatched row from either input stream, filling out the row with nulls
   </ul>
 (Nothing else is supported currently).
<li><b>int leftRowPosition:</b> Position in bindings to be used for storing a row from the left stream.
<li><b>int rightRowPosition:</b> Position in bindings to be used for storing a row from the right stream.

 <h1>Behavior</h1>

 The two streams are merged, looking for pairs of rows, one from each input stream, which match in the common
 fields. When such a match is found, an IntersectRow is created, encapuslating the matching input rows.

 <h1>Output</h1>

 IntersectRows whose inputs match in the common fields.

 <h1>Assumptions</h1>

 Each input stream is ordered by the last <tt>orderingFields</tt> columns.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 Two input rows, one from each stream.

 */

class IntersectCrossBranch_Ordered extends Operator
{
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public IntersectInBranchRowType rowType()
    {
        return intersectRowType;
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

    public IntersectCrossBranch_Ordered(Operator left,
                                        Operator right,
                                        IndexRowType leftRowType,
                                        IndexRowType rightRowType,
                                        int leftOrderingFields,
                                        int rightOrderingFields,
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
        ArgumentValidation.isGT("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGT("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        this.left = left;
        this.right = right;
        this.leftRowPosition = leftRowPosition;
        this.rightRowPosition = rightRowPosition;
        this.intersectRowType = leftRowType.schema().newIntersectCrossBranchType(leftRowType, rightRowType);
        // Setup for row comparisons
        advanceLeftOnMatch = leftOrderingFields >= rightOrderingFields;
        advanceRightOnMatch = rightOrderingFields >= leftOrderingFields;
        int commonFields = min(leftOrderingFields, rightOrderingFields);
        fieldRankingExpressions = new RankExpression[commonFields];
        int leftField = leftRowType.nFields() - leftOrderingFields;
        int rightField = rightRowType.nFields() - rightOrderingFields;
        for (int f = 0; f < commonFields; f++) {
            fieldRankingExpressions[f] =
                new RankExpression(
                    new BoundFieldExpression(leftRowPosition, new FieldExpression(leftRowType, leftField)),
                    new BoundFieldExpression(rightRowPosition, new FieldExpression(rightRowType, rightField)));
            leftField++;
            rightField++;
        }
        // outerjoins
        keepUnmatchedLeft = joinType == JoinType.LEFT_JOIN || joinType == JoinType.FULL_JOIN;
        keepUnmatchedRight = joinType == JoinType.RIGHT_JOIN || joinType == JoinType.FULL_JOIN;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: IntersectInBranch_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: IntersectInBranch_Ordered next");

    // Object state

    private final Operator left;
    private final Operator right;
    private final IntersectInBranchRowType intersectRowType;
    private final int leftRowPosition;
    private final int rightRowPosition;
    private final RankExpression[] fieldRankingExpressions;
    private final boolean advanceLeftOnMatch;
    private final boolean advanceRightOnMatch;
    private final boolean keepUnmatchedLeft;
    private final boolean keepUnmatchedRight;

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
                closed = leftRow.isEmpty() && rightRow.isEmpty();
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
                    assert !(leftRow.isEmpty() && rightRow.isEmpty());
                    long c = compareRows();
                    if (c < 0) {
                        if (keepUnmatchedLeft) {
                            next = leftOnlyRow();
                        }
                        nextLeftRow();
                    } else if (c > 0) {
                        if (keepUnmatchedRight) {
                            next = rightOnlyRow();
                        }
                        nextRightRow();
                    } else {
                        next = combineRows();
                        if (advanceLeftOnMatch) {
                            nextLeftRow();
                        }
                        if (advanceRightOnMatch) {
                            nextRightRow();
                        }
                    }
                    boolean leftEmpty = leftRow.isEmpty();
                    boolean rightEmpty = rightRow.isEmpty();
                    if (leftEmpty && rightEmpty ||
                        leftEmpty && !keepUnmatchedRight ||
                        rightEmpty && !keepUnmatchedLeft) {
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
                    fieldRankingEvaluation.of(context);
                    c = fieldRankingEvaluation.eval().getInt();
                    if (c != 0) {
                        break;
                    }
                }
            }
            return c;
        }
        
        private Row leftOnlyRow()
        {
            return new IntersectRow(intersectRowType, leftRow.get(), null);
        }
        
        private Row rightOnlyRow()
        {
            return new IntersectRow(intersectRowType, null, rightRow.get());
        }

        private Row combineRows()
        {
            return new IntersectRow(intersectRowType, leftRow.get(), rightRow.get());
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
