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
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.std.AbstractTwoArgExpressionEvaluation;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.akiban.qp.operator.API.IntersectOutputOption;
import static com.akiban.qp.operator.API.JoinType;
import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 Intersect_Ordered finds pairs of rows from two input streams whose projection onto a set of common fields matches.
 Each input stream must be ordered by these common fields.
 For each pair, an output row of IntersectRowType, comprising both input rows, is generated.
 <p>Example: Suppose the left input is an index on (x, pid, cid) and the right input is an index on (a, b, pid, cid).
 The left index scan has restricted the value of x, and the right index scan has restricted the values of a and b.
 Both streams are therefore ordered by (pid, cid).

 <h1>Arguments</h1>

<li><b>Operator left:</b> Operator providing left input stream.
<li><b>Operator right:</b> Operator providing right input stream.
<li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
<li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
<li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
<li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
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

 IntersectRows whose inputs match in the common fields. The common
 fields are the first <tt>min(leftOrderingFields,
 rightOrderingFields)</tt> of each row's ordering fields. Example:
 <ul>
    <li>The left input is (x, pid, cid).
    <li>leftOrderingFields is 2, (covering pid, cid).
    <li>The right input is (a, b, pid).
    <li>rightOrderingFields is 1 (covering pid).
 </ul>
 The common fields are (pid) from the left input, and (pid) from the right input.

 <h1>Assumptions</h1>

 Each input stream is ordered by its ordering columns, as determined by <tt>leftOrderingFields</tt>
 and <tt>rightOrderingFields</tt>.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 Two input rows, one from each stream.

 */

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
                             RowType leftRowType,
                             RowType rightRowType,
                             int leftOrderingFields,
                             int rightOrderingFields,
                             int comparisonFields,
                             JoinType joinType,
                             IntersectOutputOption intersectOutput)
    {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftRowType);
        ArgumentValidation.notNull("rightRowType", rightRowType);
        ArgumentValidation.notNull("joinType", joinType);
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("comparisonFields", comparisonFields, 0);
        ArgumentValidation.isLTE("comparisonFields", comparisonFields, min(leftOrderingFields, rightOrderingFields));
        ArgumentValidation.isNotSame("joinType", joinType, "JoinType.FULL_JOIN", JoinType.FULL_JOIN);
        ArgumentValidation.isTrue("joinType consistent with intersectOutput",
                                  joinType == JoinType.INNER_JOIN ||
                                  joinType == JoinType.LEFT_JOIN && intersectOutput == IntersectOutputOption.OUTPUT_LEFT ||
                                  joinType == JoinType.RIGHT_JOIN && intersectOutput == IntersectOutputOption.OUTPUT_RIGHT);
        this.left = left;
        this.right = right;
        this.advanceLeftOnMatch = leftOrderingFields >= rightOrderingFields;
        this.advanceRightOnMatch = rightOrderingFields >= leftOrderingFields;
        // outerjoins
        this.keepUnmatchedLeft = joinType == JoinType.LEFT_JOIN;
        this.keepUnmatchedRight = joinType == JoinType.RIGHT_JOIN;
        // output
        this.outputLeft = intersectOutput == IntersectOutputOption.OUTPUT_LEFT;
            // Setup for row comparisons
        this.fieldRankingExpressions = new RankExpression[comparisonFields];
        int leftField = leftRowType.nFields() - leftOrderingFields;
        int rightField = rightRowType.nFields() - rightOrderingFields;
        for (int f = 0; f < comparisonFields; f++) {
            this.fieldRankingExpressions[f] =
                new RankExpression(new FieldExpression(leftRowType, leftField),
                                   new FieldExpression(rightRowType, rightField));
            leftField++;
            rightField++;
        }
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered next");

    // Object state

    private final Operator left;
    private final Operator right;
    private final RankExpression[] fieldRankingExpressions;
    private final boolean advanceLeftOnMatch;
    private final boolean advanceRightOnMatch;
    private final boolean keepUnmatchedLeft;
    private final boolean keepUnmatchedRight;
    private final boolean outputLeft;

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
                            assert outputLeft;
                            next = leftRow.get();
                        }
                        nextLeftRow();
                    } else if (c > 0) {
                        if (keepUnmatchedRight) {
                            assert !outputLeft;
                            next = rightRow.get();
                        }
                        nextRightRow();
                    } else {
                        next = (outputLeft ? leftRow : rightRow).get();
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
            fieldRankingEvaluations = new AbstractTwoArgExpressionEvaluation[fieldRankingExpressions.length];
            for (int f = 0; f < fieldRankingEvaluations.length; f++) {
                fieldRankingEvaluations[f] = 
                    (AbstractTwoArgExpressionEvaluation) fieldRankingExpressions[f].evaluation();
            }
        }
        
        // For use by this class
        
        private void nextLeftRow()
        {
            Row row = leftInput.next();
            leftRow.hold(row);
        }
        
        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow.hold(row);
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
                for (AbstractTwoArgExpressionEvaluation fieldRankingEvaluation : fieldRankingEvaluations) {
                    fieldRankingEvaluation.leftEvaluation().of(leftRow.get());
                    fieldRankingEvaluation.rightEvaluation().of(rightRow.get());
                    c = fieldRankingEvaluation.eval().getInt();
                    if (c != 0) {
                        break;
                    }
                }
            }
            return c;
        }
        
        // Object state
        
        // Rows from each input stream are bound to the QueryContext. However, QueryContext doesn't use
        // ShareHolders, so they are needed here.

        private boolean closed = false;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private final ShareHolder<Row> leftRow = new ShareHolder<Row>();
        private final ShareHolder<Row> rightRow = new ShareHolder<Row>();
        private final AbstractTwoArgExpressionEvaluation[] fieldRankingEvaluations;
    }
}
