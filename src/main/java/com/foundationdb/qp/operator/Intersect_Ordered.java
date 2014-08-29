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

import com.foundationdb.server.error.SetWrongNumColumns;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.foundationdb.qp.operator.API.IntersectOption;
import static com.foundationdb.qp.operator.API.JoinType;
import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 * <h1>Overview</h1>
 * <p/>
 * Intersect_Ordered finds rows from one of two input streams whose projection onto a set of common fields matches
 * a row in the other stream. Each input stream must be ordered by at least these common fields.
 * For each matching pair of rows, output from the selected input stream is emitted as output.
 * <p/>
 * <h1>Arguments</h1>
 * <p/>
 * <li><b>Operator left:</b> Operator providing left input stream.
 * <li><b>Operator right:</b> Operator providing right input stream.
 * <li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
 * <li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
 * <li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
 * <li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
 * <li><b>boolean[] ascending:</b> The length of this array specifies the number of fields to be compared in the merge
 * for the purpose of determining whether a left row and right row agree and will result in an output row,
 * ascending.length <= min(leftOrderingFields, rightOrderingFields). ascending[i] is true if the ith such field
 * is ascending, false if it is descending. This ordering specification must be consistent with the order of both
 * input streams.
 * <li><b>JoinType joinType:</b>
 * <li><b>boolean outputEqual:</b>Used to allow intersect operator to output all instances of a match in the left stream even
 * if only one instance of a match exists in the right stream.
 * <ul>
 * <li>INNER_JOIN: An ordinary intersection is computed.
 * <li>LEFT_JOIN: Keep an unmatched row from the left input stream, filling out the row with nulls
 * <li>RIGHT_JOIN: Keep an unmatched row from the right input stream, filling out the row with nulls
 * <li>FULL_JOIN: Not supported
 * </ul>
 * (Nothing else is supported currently).
 * <li><b>IntersectOption intersectOutput:</b> OUTPUT_LEFT or OUTPUT_RIGHT, depending on which streams rows
 * should be emitted as output.
 * <p/>
 * <h1>Behavior</h1>
 * <p/>
 * The two streams are merged, looking for pairs of rows, one from each input stream, which match in the common
 * fields. When such a match is found, a row from the stream selected by <tt>intersectOutput</tt> is emitted.
 * <p/>
 * <h1>Output</h1>
 * <p/>
 * Rows that match at least one row in the other input stream.
 * <p/>
 * <h1>Assumptions</h1>
 * <p/>
 * Each input stream is ordered by its ordering columns, as determined by <tt>leftOrderingFields</tt>
 * and <tt>rightOrderingFields</tt>, and ordered according to <tt>ascending</tt>. The order of rows in both
 * input streams must be consistent with <tt>ascending.</tt>
 * <p/>
 * The input row types must correspond to indexes in the same group (as determined by the index's leafmost table).
 * This constraint may be relaxed in the future, but then the number of fields to compare is likely to be required
 * as a new constructor argument.
 * <p/>
 * <h1>Performance</h1>
 * <p/>
 * This operator does no IO.
 * <p/>
 * <h1>Memory Requirements</h1>
 * <p/>
 * Two input rows, one from each stream.
 */

class Intersect_Ordered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(skip %d from left, skip %d from right, compare %d%s)",
                             getClass().getSimpleName(), leftFixedFields, rightFixedFields, fieldsToCompare, skipScan ? ", SKIP_SCAN" : "");
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
        right.findDerivedTypes(derivedTypes);
        left.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(2);
        result.add(left);
        result.add(right);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(left), describePlan(right));
    }

    // Intersect_Ordered interface

    public Intersect_Ordered(Operator left,
                             Operator right,
                             RowType leftRowType,
                             RowType rightRowType,
                             int leftOrderingFields,
                             int rightOrderingFields,
                             boolean[] ascending,
                             JoinType joinType,
                             EnumSet<IntersectOption> options,
                             List<TComparison> comparisons,
                             boolean outputEqual)
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
        ArgumentValidation.notNull("joinType", joinType);
        ArgumentValidation.notNull("options", options);
        ArgumentValidation.notNull("outputEqual", outputEqual);
        if(!outputEqual) {
            ArgumentValidation.isEQ("leftOrderingFields", leftOrderingFields, "rightOrderingFields", rightOrderingFields);
            if (leftRowType.nFields() != rightRowType.nFields()) {
                throw new SetWrongNumColumns(leftRowType.nFields(), rightRowType.nFields());
            }
        }

        // scan algorithm
        boolean skipScan = options.contains(IntersectOption.SKIP_SCAN);
        boolean sequentialScan = options.contains(IntersectOption.SEQUENTIAL_SCAN);
        // skip scan is the default until everyone is explicit about it
        if (!skipScan && !sequentialScan) {
            skipScan = true;
        }
        ArgumentValidation.isTrue("options for scanning",
                                  (skipScan || sequentialScan) &&
                                  !(skipScan && sequentialScan));
        this.skipScan = skipScan;
        // output
        this.outputLeft = options.contains(IntersectOption.OUTPUT_LEFT);
        boolean outputRight = options.contains(IntersectOption.OUTPUT_RIGHT);
        ArgumentValidation.isTrue("options for output",
                                  (outputLeft || outputRight) &&
                                  !(outputLeft && outputRight));
        ArgumentValidation.isTrue("joinType consistent with intersectOutput",
                                  joinType == JoinType.INNER_JOIN ||
                                  joinType == JoinType.LEFT_JOIN && options.contains(IntersectOption.OUTPUT_LEFT) ||
                                  joinType == JoinType.RIGHT_JOIN && options.contains(IntersectOption.OUTPUT_RIGHT));
        this.left = left;
        this.right = right;
        this.leftRowType = leftRowType;
        this.rightRowType = rightRowType;
        this.joinType = joinType;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        // outerjoins
        this.keepUnmatchedLeft = joinType == JoinType.LEFT_JOIN;
        this.keepUnmatchedRight = joinType == JoinType.RIGHT_JOIN;
        // Setup for row comparisons
        this.leftFixedFields = leftRowType.nFields() - leftOrderingFields;
        this.rightFixedFields = rightRowType.nFields() - rightOrderingFields;
        this.fieldsToCompare = ascending.length;
        // Setup for jumping
        leftSkipRowColumnSelector = new IndexRowPrefixSelector(leftFixedFields + fieldsToCompare);
        rightSkipRowColumnSelector = new IndexRowPrefixSelector(rightFixedFields + fieldsToCompare);
        ArgumentValidation.isTrue("comparisons", (comparisons == null) || (fieldsToCompare == comparisons.size()));
        this.comparisons = comparisons;
        this.outputEqual = outputEqual;
    }

    // For use by this class

    private static int compare(List<TComparison> comparisons, int count, Row left, int leftOff, Row right, int rightOff) {
        if(comparisons == null) {
            return left.compareTo(right, leftOff, rightOff, count);
        }
        RowType lType = left.rowType();
        RowType rType = right.rowType();
        int li = leftOff;
        int ri = rightOff;
        int c = 0;
        for(int i = 0; (c == 0) && (i < count); ++i, li++, ri++) {
            TComparison comp = comparisons.get(i);
            if(comp == null) {
                c = left.compareTo(right, li, ri, 1);
            } else {
                c = comp.compare(lType.typeAt(li), left.value(li), rType.typeAt(ri), right.value(ri));
            }
        }
        return c;
    }
    @Override
    public RowType rowType(){
        if(outputLeft)
            return leftRowType;
        return rightRowType;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: intersect_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private final RowType leftRowType;
    private final RowType rightRowType;
    private final JoinType joinType;
    private final int leftFixedFields;
    private final int rightFixedFields;
    private final int fieldsToCompare;
    private final boolean keepUnmatchedLeft;
    private final boolean keepUnmatchedRight;
    private final boolean outputLeft;
    private final boolean skipScan;
    private final boolean[] ascending;
    private final ColumnSelector leftSkipRowColumnSelector;
    private final ColumnSelector rightSkipRowColumnSelector;
    private final List<TComparison> comparisons;
    private final boolean outputEqual;
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(leftFixedFields));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(rightFixedFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(fieldsToCompare));
        atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance(joinType.name().replace("_JOIN", "")));
        atts.put(Label.INPUT_OPERATOR, left.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, right.getExplainer(context));
        return new CompoundExplainer(Type.ORDERED, atts);
    }

    // Inner classes

    private class Execution extends OperatorCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                leftInput.open();
                rightInput.open();
                nextLeftRow();
                nextRightRow();
                closed = leftRow == null && rightRow == null;
                leftSkipRowFixed = rightSkipRowFixed = false; // Fixed fields are per iteration.
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
                Row next = null;
                while (!closed && next == null) {
                    assert !(leftRow == null && rightRow == null);
                    long c = compareRows();
                    if (c < 0) {
                        if (keepUnmatchedLeft) {
                            assert outputLeft;
                            next = leftRow;
                            nextLeftRow();
                        } else {
                            if (skipScan) {
                                nextLeftRowSkip(rightRow, rightFixedFields, leftSkipRowColumnSelector, false);
                            } else {
                                nextLeftRow();
                            }
                        }
                    } else if (c > 0) {
                        if (keepUnmatchedRight) {
                            assert !outputLeft;
                            next = rightRow;
                            nextRightRow();
                        } else {
                            if (skipScan) {
                                nextRightRowSkip(leftRow, leftFixedFields, rightSkipRowColumnSelector, false);
                            } else {
                                nextRightRow();
                            }
                        }
                    } else {
                        // left and right rows match
                        if (outputLeft) {
                            next = leftRow;
                            if(!outputEqual) {
                                nextRightRow();
                            }
                            nextLeftRow();
                        } else {
                            next = rightRow;
                            if(!outputEqual) {
                                nextLeftRow();
                            }
                            nextRightRow();
                        }
                    }
                    boolean leftEmpty = leftRow == null;
                    boolean rightEmpty = rightRow == null;
                    if (leftEmpty && rightEmpty ||
                        leftEmpty && !keepUnmatchedRight ||
                        rightEmpty && !keepUnmatchedLeft) {
                        close();
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("intersect_Ordered: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void jump(Row jumpRow, ColumnSelector jumpRowColumnSelector)
        {
            // This operator emits rows from left or right. The row used to specify the jump should be of the matching
            // row type.
            int suffixRowFixedFields;
            if (outputLeft) {
                assert jumpRow.rowType() == leftRowType : jumpRow.rowType();
                suffixRowFixedFields = leftFixedFields;
            } else {
                assert jumpRow.rowType() == rightRowType : jumpRow.rowType();
                suffixRowFixedFields = rightFixedFields;
            }
            nextLeftRowSkip(jumpRow, suffixRowFixedFields, jumpRowColumnSelector, true);
            nextRightRowSkip(jumpRow, suffixRowFixedFields, jumpRowColumnSelector, true);
            if (leftRow == null || rightRow == null) {
                close();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                leftRow = null;
                rightRow = null;
                leftInput.close();
                rightInput.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            leftInput.destroy();
            rightInput.destroy();
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
            assert leftInput.isDestroyed() == rightInput.isDestroyed();
            return leftInput.isDestroyed();
        }

        @Override
        public void openBindings() {
            bindingsCursor.openBindings();
            leftInput.openBindings();
            rightInput.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            
            QueryBindings bindings = bindingsCursor.nextBindings();
            QueryBindings other = leftInput.nextBindings();
            assert (bindings == other);
            other = rightInput.nextBindings();
            assert (bindings == other);
            return bindings;
        }

        @Override
        public void closeBindings() {
            bindingsCursor.closeBindings();
            leftInput.closeBindings();
            rightInput.closeBindings();
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            leftInput.cancelBindings(bindings);
            rightInput.cancelBindings(bindings);
            bindingsCursor.cancelBindings(bindings);
            
            // Close this cursor, but don't clear the state
            // In look ahead cases the cursor will be reopened
            // with state intact for further reading. 
            closed = true;
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
            this.bindingsCursor = multiple;
            this.leftInput = left.cursor(context, multiple.newCursor());
            this.rightInput = right.cursor(context, multiple.newCursor());
        }

        // For use by this class

        private void nextLeftRow()
        {
            Row row = leftInput.next();
            leftRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("intersect_Ordered: left {}", row);
            }
        }

        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("intersect_Ordered: right {}", row);
            }
        }

        private int compareRows()
        {
            int c;
            assert !closed;
            assert !(leftRow == null && rightRow == null);
            if (leftRow == null) {
                c = 1;
            } else if (rightRow == null) {
                c = -1;
            } else {
                c = leftRow.compareTo(rightRow, leftFixedFields, rightFixedFields, fieldsToCompare);
                c = adjustComparison(c);

            }
            return c;
        }

        private int adjustComparison(int c) 
        {
            if (c != 0) {
                int fieldThatDiffers = abs(c) - 1;
                assert fieldThatDiffers < ascending.length;
                if (!ascending[fieldThatDiffers]) {
                    c = -c;
                }
            }
            return c;
        }

        private void nextLeftRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector, boolean check)
        {
            if (leftRow != null) {
                if (check) {
                    int c = compare(comparisons, fieldsToCompare, leftRow, leftFixedFields, jumpRow, jumpRowFixedFields);
                    c = adjustComparison(c);
                    if (c >= 0) return;
                }
                addSuffixToSkipRow(leftSkipRow(),
                                   leftFixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                leftInput.jump(leftSkipRow, jumpRowColumnSelector);
                leftRow = leftInput.next();
            }
        }

        private void nextRightRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector, boolean check)
        {
            if (rightRow != null) {
                if (check) {
                    int c = compare(comparisons, fieldsToCompare, jumpRow, jumpRowFixedFields, rightRow, rightFixedFields);
                    c = adjustComparison(c);
                    if (c >= 0) return;
                }
                addSuffixToSkipRow(rightSkipRow(),
                                   rightFixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                rightInput.jump(rightSkipRow, jumpRowColumnSelector);
                rightRow = rightInput.next();
            }
        }

        private void addSuffixToSkipRow(ValuesHolderRow skipRow,
                                        int skipRowFixedFields,
                                        Row jumpRow,
                                        int jumpRowFixedFields)
        {
            if (jumpRow == null) {
                for (int f = 0; f < fieldsToCompare; f++) {
                    skipRow.valueAt(skipRowFixedFields + f).putNull();
                }
            } else {
                for (int f = 0; f < fieldsToCompare; f++) {
                    TComparison comparison = null;
                    if (comparisons != null && (comparison = comparisons.get(f)) != null)
                        comparison.copyComparables(jumpRow.value(jumpRowFixedFields + f),
                                                   skipRow.valueAt(skipRowFixedFields + f));
                    else
                        ValueTargets.copyFrom(
                                jumpRow.value(jumpRowFixedFields + f),
                                skipRow.valueAt(skipRowFixedFields + f));
                }
            }
        }

        private ValuesHolderRow leftSkipRow()
        {
            if (!leftSkipRowFixed) {
                if (leftSkipRow == null)
                    leftSkipRow = new ValuesHolderRow(leftRowType);
                assert leftRow != null;
                int f = 0;
                while (f < leftFixedFields) {
                    ValueTargets.copyFrom(
                            leftRow.value(f),
                            leftSkipRow.valueAt(f));
                    f++;
                }
                while (f < leftRowType.nFields()) {
                    leftSkipRow.valueAt(f++).putNull();
                }
                leftSkipRowFixed = true;
            }
            return leftSkipRow;
        }

        private ValuesHolderRow rightSkipRow()
        {
            if (!rightSkipRowFixed) {
                if (rightSkipRow == null)
                    rightSkipRow = new ValuesHolderRow(rightRowType);
                assert rightRow != null;
                int f = 0;
                while (f < rightFixedFields) {
                    ValueTargets.copyFrom(
                            rightRow.value(f),
                            rightSkipRow.valueAt(f));
                    f++;
                }
                while (f < rightRowType.nFields()) {
                    rightSkipRow.valueAt(f++).putNull();
                }
                rightSkipRowFixed = true;
            }
            return rightSkipRow;
        }

        // Object state

        private boolean closed = true;
        private final QueryBindingsCursor bindingsCursor;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private Row leftRow;
        private Row rightRow;
        private ValuesHolderRow leftSkipRow;
        private ValuesHolderRow rightSkipRow;
        private boolean leftSkipRowFixed;
        private boolean rightSkipRowFixed;
    }
}
