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
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types3.pvalue.PValueTargets;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.ShareHolder;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 Union_Ordered combines rows from two input streams. The input streams must be based on the same index, (a restriction
 that we expect to drop in the future). Duplicate rows are suppressed.

 <h1>Arguments</h1>

<li><b>Operator left:</b> Operator providing left input stream.
<li><b>Operator right:</b> Operator providing right input stream.
<li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
<li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
<li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
<li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
<li><b>boolean[] ascending:</b> The length of this arrays specifies the number of fields to be compared in the merge,
 (<= min(leftOrderingFields, rightOrderingFields). ascending[i] is true if the ith such field is ascending, false
 if it is descending.

 <h1>Behavior</h1>

 The two streams are merged, yielding an output stream ordered compatibly with the input streams.

 <h1>Output</h1>

 All input rows.

 <h1>Assumptions</h1>

 Each input stream is ordered by its ordering columns, as determined by <tt>leftOrderingFields</tt>
 and <tt>rightOrderingFields</tt>.

 For now: leftRowType == inputRowType and leftOrderingFields == rightOrderingFields.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 Two input rows, one from each stream.

 */

class Union_Ordered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(skip %d, compare %d)",
                             getClass().getSimpleName(), fixedFields, fieldsToCompare);
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

    // Union_Ordered interface

    public Union_Ordered(Operator left,
                         Operator right,
                         IndexRowType leftRowType,
                         IndexRowType rightRowType,
                         int leftOrderingFields,
                         int rightOrderingFields,
                         boolean[] ascending,
                         boolean outputEqual,
                         boolean usePValues)
    {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftRowType);
        ArgumentValidation.notNull("rightRowType", rightRowType);
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("ascending.length()", ascending.length, 0);
        ArgumentValidation.isLTE("ascending.length()", ascending.length, min(leftOrderingFields, rightOrderingFields));
        // The following assumptions will be relaxed when this operator is generalized to support inputs from different
        // indexes.
        ArgumentValidation.isEQ("leftRowType", leftRowType, "rightRowType", rightRowType);
        ArgumentValidation.isEQ("leftOrderingFields", leftOrderingFields, "rightOrderingFields", rightOrderingFields);
        this.left = left;
        this.right = right;
        this.rowType = leftRowType;
        // Setup for row comparisons
        this.fixedFields = rowType.nFields() - leftOrderingFields;
        this.fieldsToCompare = leftOrderingFields;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        this.outputEqual = outputEqual;
        // TODO (in Execution): Check that ascending bits are consistent with IndexCursor directions.
        this.usePValues = usePValues;
    }

    // For use by this class

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Union_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Union_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Union_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private IndexRowType rowType;
    private final int fixedFields;
    private final int fieldsToCompare;
    private final boolean[] ascending;
    private final boolean outputEqual;
    private final boolean usePValues;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(fixedFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(fieldsToCompare));
        atts.put(Label.INPUT_OPERATOR, left.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, right.getExplainer(context));
        if (outputEqual)
            atts.put(Label.UNION_OPTION, PrimitiveExplainer.getInstance("ALL"));
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
                closed = false;
                if (leftRow.isEmpty() && rightRow.isEmpty()) {
                    close();
                }
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
                if (isActive()) {
                    assert !(leftRow.isEmpty() && rightRow.isEmpty());
                    int c = compareRows();
                    if (c < 0) {
                        next = leftRow.get();
                        nextLeftRow();
                    } else if (c > 0) {
                        next = rightRow.get();
                        nextRightRow();
                    } else {
                        // left and right rows match. Output at least one.
                        next = leftRow.get();
                        nextLeftRow();
                        if (!outputEqual)
                            nextRightRow();
                    }
                    if (leftRow.isEmpty() && rightRow.isEmpty()) {
                        close();
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Union_Ordered: yield {}", next);
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
            nextLeftRowSkip(jumpRow, fixedFields, jumpRowColumnSelector);
            nextRightRowSkip(jumpRow, fixedFields, jumpRowColumnSelector);
            if (leftRow.isEmpty() && rightRow.isEmpty()) {
                close();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                leftRow.release();
                rightRow.release();
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
            leftRow.hold(row);
            if (LOG_EXECUTION) {
                LOG.debug("Union_Ordered: left {}", row);
            }
        }
        
        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow.hold(row);
            if (LOG_EXECUTION) {
                LOG.debug("Union_Ordered: right {}", row);
            }
        }
        
        private int compareRows()
        {
            int c;
            assert !closed;
            assert !(leftRow.isEmpty() && rightRow.isEmpty());
            if (leftRow.isEmpty()) {
                c = 1;
            } else if (rightRow.isEmpty()) {
                c = -1;
            } else {
                c = leftRow.get().compareTo(rightRow.get(), fixedFields, fixedFields, fieldsToCompare);
                c = adjustComparison(c);
            }
            return c;
        }

        private int adjustComparison(int c) 
        {
            if (c != 0) {
                int fieldThatDiffers = abs(c) - 1;
                if (!ascending[fieldThatDiffers]) {
                    c = -c;
                }
            }            
            return c;
        }

        private void nextLeftRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector)
        {
            if (leftRow.isHolding()) {
                int c = leftRow.get().compareTo(jumpRow, fixedFields, jumpRowFixedFields, fieldsToCompare);
                c = adjustComparison(c);
                if (c >= 0) return;
                addSuffixToSkipRow(leftSkipRow(),
                                   fixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                leftInput.jump(leftSkipRow, jumpRowColumnSelector);
                leftRow.hold(leftInput.next());
            }
        }

        private void nextRightRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector)
        {
            if (rightRow.isHolding()) {
                int c = rightRow.get().compareTo(jumpRow, fixedFields, jumpRowFixedFields, fieldsToCompare);
                c = adjustComparison(c);
                if (c >= 0) return;
                addSuffixToSkipRow(rightSkipRow(),
                                   fixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                rightInput.jump(rightSkipRow, jumpRowColumnSelector);
                rightRow.hold(rightInput.next());
            }
        }

        private void addSuffixToSkipRow(ValuesHolderRow skipRow,
                                        int skipRowFixedFields,
                                        Row suffixRow,
                                        int suffixRowFixedFields)
        {
            boolean usingPValues = Union_Ordered.this.usePValues;
            if (suffixRow == null) {
                for (int f = 0; f < fieldsToCompare; f++) {
                    if (usingPValues)
                        skipRow.pvalueAt(skipRowFixedFields + f).putNull();
                    else
                        skipRow.holderAt(skipRowFixedFields + f).putNull();
                }
            } else {
                for (int f = 0; f < fieldsToCompare; f++) {
                    if (usingPValues)
                        PValueTargets.copyFrom(suffixRow.pvalue(
                                suffixRowFixedFields + f),
                                skipRow.pvalueAt(skipRowFixedFields + f));
                    else
                        skipRow.holderAt(skipRowFixedFields + f).copyFrom(suffixRow.eval(suffixRowFixedFields + f));
                }
            }
        }

        private ValuesHolderRow leftSkipRow()
        {
            if (!leftSkipRowFixed) {
                boolean usingPValues = Union_Ordered.this.usePValues;
                if (leftSkipRow == null)
                    leftSkipRow = new ValuesHolderRow(rowType, usingPValues);
                assert leftRow.isHolding();
                int f = 0;
                while (f < fixedFields) {
                    if (usingPValues)
                        PValueTargets.copyFrom(leftRow.get().pvalue(f), leftSkipRow.pvalueAt(f));
                    else
                        leftSkipRow.holderAt(f).copyFrom(leftRow.get().eval(f));
                    f++;
                }
                while (f < rowType.nFields()) {
                    if (usingPValues)
                        leftSkipRow.pvalueAt(f++).putNull();
                    else
                        leftSkipRow.holderAt(f++).putNull();
                }
                leftSkipRowFixed = true;
            }
            return leftSkipRow;
        }

        private ValuesHolderRow rightSkipRow()
        {
            if (!rightSkipRowFixed) {
                boolean usingPValues = Union_Ordered.this.usePValues;
                if (rightSkipRow == null)
                    rightSkipRow = new ValuesHolderRow(rowType, usingPValues);
                assert rightRow.isHolding();
                int f = 0;
                while (f < fixedFields) {
                    if (usingPValues)
                        PValueTargets.copyFrom(rightRow.get().pvalue(f), rightSkipRow.pvalueAt(f));
                    else
                        rightSkipRow.holderAt(f).copyFrom(rightRow.get().eval(f));
                    f++;
                }
                while (f < rowType.nFields()) {
                    if (usingPValues)
                        rightSkipRow.pvalueAt(f++).putNull();
                    else
                        rightSkipRow.holderAt(f++).putNull();
                }
                rightSkipRowFixed = true;
            }
            return rightSkipRow;
        }

        // Object state
        
        // Rows from each input stream are bound to the QueryContext. However, QueryContext doesn't use
        // ShareHolders, so they are needed here.

        private boolean closed = true;
        private final QueryBindingsCursor bindingsCursor;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private final ShareHolder<Row> leftRow = new ShareHolder<>();
        private final ShareHolder<Row> rightRow = new ShareHolder<>();
        private ValuesHolderRow leftSkipRow;
        private ValuesHolderRow rightSkipRow;
        private boolean leftSkipRowFixed;
        private boolean rightSkipRowFixed;
    }
}
