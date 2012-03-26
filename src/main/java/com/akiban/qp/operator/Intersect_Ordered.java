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
import com.akiban.server.expression.std.AbstractTwoArgExpressionEvaluation;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.akiban.qp.operator.API.IntersectOutputOption;
import static com.akiban.qp.operator.API.JoinType;
import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 Intersect_Ordered finds rows from one of two input streams whose projection onto a set of common fields matches
 a row in the other stream. Each input stream must be ordered by at least these common fields.
 For each matching pair of rows, output from the selected input stream is emitted as output.

 <h1>Arguments</h1>

<li><b>Operator left:</b> Operator providing left input stream.
<li><b>Operator right:</b> Operator providing right input stream.
<li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
<li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
<li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
<li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
<li><b>int comparisonFields:</b> Number of ordering fields to be compared.
<li><b>JoinType joinType:</b>
   <ul>
     <li>INNER_JOIN: An ordinary intersection is computed.
     <li>LEFT_JOIN: Keep an unmatched row from the left input stream, filling out the row with nulls
     <li>RIGHT_JOIN: Keep an unmatched row from the right input stream, filling out the row with nulls
     <li>FULL_JOIN: Not supported
   </ul>
 (Nothing else is supported currently).
<li><b>IntersectOutputOption intersectOutput:</b> OUTPUT_LEFT or OUTPUT_RIGHT, depending on which streams rows
 should be emitted as output.

 <h1>Behavior</h1>

 The two streams are merged, looking for pairs of rows, one from each input stream, which match in the common
 fields. When such a match is found, a row from the stream selected by <tt>intersectOutput</tt> is emitted.

 <h1>Output</h1>

 Rows that match at least one row in the other input stream.

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
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(skip %d from left, skip %d from right, compare %d)",
                             getClass().getSimpleName(), leftSkip, rightSkip, fieldRankingExpressions.length);
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
                             boolean[] ascending,
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
        ArgumentValidation.isGTE("ascending.length()", ascending.length, 0);
        ArgumentValidation.isLTE("ascending.length()", ascending.length, min(leftOrderingFields, rightOrderingFields));
        ArgumentValidation.isNotSame("joinType", joinType, "JoinType.FULL_JOIN", JoinType.FULL_JOIN);
        ArgumentValidation.notNull("intersectOutput", intersectOutput);
        ArgumentValidation.isTrue("joinType consistent with intersectOutput",
                                  joinType == JoinType.INNER_JOIN ||
                                  joinType == JoinType.LEFT_JOIN && intersectOutput == IntersectOutputOption.OUTPUT_LEFT ||
                                  joinType == JoinType.RIGHT_JOIN && intersectOutput == IntersectOutputOption.OUTPUT_RIGHT);
        this.left = left;
        this.right = right;
        this.ascending = ascending;
        // outerjoins
        this.keepUnmatchedLeft = joinType == JoinType.LEFT_JOIN;
        this.keepUnmatchedRight = joinType == JoinType.RIGHT_JOIN;
        // output
        this.outputLeft = intersectOutput == IntersectOutputOption.OUTPUT_LEFT;
        // Setup for row comparisons
        this.fieldRankingExpressions = new RankExpression[ascending.length];
        leftSkip = leftRowType.nFields() - leftOrderingFields;
        rightSkip = rightRowType.nFields() - rightOrderingFields;
        for (int f = 0; f < fieldRankingExpressions.length; f++) {
            this.fieldRankingExpressions[f] =
                new RankExpression(new FieldExpression(leftRowType, leftSkip + f),
                                   new FieldExpression(rightRowType, rightSkip + f));
        }
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private final int leftSkip;
    private final int rightSkip;
    private final RankExpression[] fieldRankingExpressions;
    private final boolean keepUnmatchedLeft;
    private final boolean keepUnmatchedRight;
    private final boolean outputLeft;
    private final boolean[] ascending;

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
                        // left and right rows match
                        if (outputLeft) {
                            next = leftRow.get();
                            nextLeftRow();
                        } else {
                            next = rightRow.get();
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Intersect_Ordered: yield {}", next);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Intersect_Ordered: left {}", row);
            }
        }
        
        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow.hold(row);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Intersect_Ordered: right {}", row);
            }
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
                for (int f = 0; f < fieldRankingEvaluations.length; f++) {
                    AbstractTwoArgExpressionEvaluation fieldRankingEvaluation = fieldRankingEvaluations[f];
                    fieldRankingEvaluation.leftEvaluation().of(leftRow.get());
                    fieldRankingEvaluation.rightEvaluation().of(rightRow.get());
                    c = fieldRankingEvaluation.eval().getInt();
                    if (c != 0) {
                        if (!ascending[f]) {
                            c = -c;
                        }
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
