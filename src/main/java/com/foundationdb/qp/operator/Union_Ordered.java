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

import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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
 <li><b>boolean outputEqual:</b>Used to turn off suppression of duplicates in the opposite stream, this is used to make
 operator act as a Union All operation on previously sorted and duplicate free streams

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

class Union_Ordered extends SetOperatorBase
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


     // Union_Ordered interface

    public Union_Ordered(Operator left,
                         Operator right,
                         RowType leftRowType,
                         RowType rightRowType,
                         int leftOrderingFields,
                         int rightOrderingFields,
                         boolean[] ascending,
                         boolean outputEqual)
    {
        super (left, leftRowType, right, rightRowType, "UNION");
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("ascending.length()", ascending.length, 0);
        ArgumentValidation.isLTE("ascending.length()", ascending.length, min(leftOrderingFields, rightOrderingFields));
        // The following assumptions will be relaxed when this operator is generalized to support inputs from different
        // indexes.
        ArgumentValidation.isEQ("leftOrderingFields", leftOrderingFields, "rightOrderingFields", rightOrderingFields);
        // Setup for row comparisons
        this.fixedFields = rowType().nFields() - leftOrderingFields;
        this.fieldsToCompare = leftOrderingFields;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        this.outputEqual = outputEqual;
        // TODO (in Execution): Check that ascending bits are consistent with IndexCursor directions.
    }

    // For use by this class

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Union_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Union_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Union_Ordered.class);

    // Object state

    private final int fixedFields;
    private final int fieldsToCompare;
    private final boolean[] ascending;
    private final boolean outputEqual;


    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(fixedFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(fieldsToCompare));
        for (Operator op : getInputOperators())
            atts.put(Label.INPUT_OPERATOR, op.getExplainer(context));
        for (RowType type : getInputTypes())
            atts.put(Label.INPUT_TYPE, type.getExplainer(context));
        if (outputEqual)
            atts.put(Label.SET_OPTION, PrimitiveExplainer.getInstance("ALL"));
        atts.put(Label.OUTPUT_TYPE, rowType().getExplainer(context));
        
        return new CompoundExplainer(Type.ORDERED, atts);
    }

    // Inner classes

    private class Execution extends MultiChainedCursor 
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                nextLeftRow();
                nextRightRow();
                if (leftRow == null && rightRow == null) {
                    setIdle();
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
                    assert !(leftRow == null && rightRow == null);
                    int c = compareRows();
                    if (c < 0) {
                        next = leftRow;
                        nextLeftRow();
                    } else if (c > 0) {
                        next = rightRow;
                        nextRightRow();
                    } else {
                        // left and right rows match. Output at least one.
                        next = leftRow;
                        nextLeftRow();
                        if (!outputEqual)
                            nextRightRow();
                    }
                    if (leftRow == null && rightRow == null) {
                        setIdle();
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Union_Ordered: yield {}", next);
                }
                
                if (next == null) {
                    return next;
                } else if (useOverlayRow()) {
                    return new OverlayingRow (next, rowType());
                } else {
                    return next;
                }
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
            if (leftRow == null && rightRow == null) {
                close();
            }
        }

        @Override
        public void close()
        {
            super.close();
            leftRow = null;
            rightRow = null;
        }

        @Override
        protected Operator left() {
            return Union_Ordered.this.left();
        }
        
        @Override
        protected Operator right() {
            return Union_Ordered.this.right();
        }
        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, bindingsCursor);
        }
        
        // For use by this class
        
        private void nextLeftRow()
        {
            Row row = leftInput.next();
            leftRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("Union_Ordered: left {}", row);
            }
        }
        
        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("Union_Ordered: right {}", row);
            }
        }
        
        private int compareRows()
        {
            int c;
            assert isActive();
            assert !(leftRow == null && rightRow == null);
            if (leftRow == null) {
                c = 1;
            } else if (rightRow == null) {
                c = -1;
            } else {
                c = leftRow.compareTo(rightRow, fixedFields, fixedFields, fieldsToCompare);
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
            if (leftRow != null) {
                int c = leftRow.compareTo(jumpRow, fixedFields, jumpRowFixedFields, fieldsToCompare);
                c = adjustComparison(c);
                if (c >= 0) return;
                addSuffixToSkipRow(leftSkipRow(),
                                   fixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                leftInput.jump(leftSkipRow, jumpRowColumnSelector);
                leftRow = leftInput.next();
            }
        }

        private void nextRightRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector)
        {
            if (rightRow != null) {
                int c = rightRow.compareTo(jumpRow, fixedFields, jumpRowFixedFields, fieldsToCompare);
                c = adjustComparison(c);
                if (c >= 0) return;
                addSuffixToSkipRow(rightSkipRow(),
                                   fixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                rightInput.jump(rightSkipRow, jumpRowColumnSelector);
                rightRow = rightInput.next();
            }
        }

        private void addSuffixToSkipRow(ValuesHolderRow skipRow,
                                        int skipRowFixedFields,
                                        Row suffixRow,
                                        int suffixRowFixedFields)
        {
            if (suffixRow == null) {
                for (int f = 0; f < fieldsToCompare; f++) {
                    skipRow.valueAt(skipRowFixedFields + f).putNull();
                }
            } else {
                for (int f = 0; f < fieldsToCompare; f++) {
                    ValueTargets.copyFrom(suffixRow.value(
                            suffixRowFixedFields + f),
                            skipRow.valueAt(skipRowFixedFields + f));
                }
            }
        }

        private ValuesHolderRow leftSkipRow()
        {
            if (!leftSkipRowFixed) {
                if (leftSkipRow == null)
                    leftSkipRow = new ValuesHolderRow(rowType());
                assert leftRow != null;
                int f = 0;
                while (f < fixedFields) {
                    ValueTargets.copyFrom(leftRow.value(f), leftSkipRow.valueAt(f));
                    f++;
                }
                while (f < rowType().nFields()) {
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
                    rightSkipRow = new ValuesHolderRow(rowType());
                assert rightRow != null;
                int f = 0;
                while (f < fixedFields) {
                    ValueTargets.copyFrom(rightRow.value(f), rightSkipRow.valueAt(f));
                    f++;
                }
                while (f < rowType().nFields()) {
                    rightSkipRow.valueAt(f++).putNull();
                }
                rightSkipRowFixed = true;
            }
            return rightSkipRow;
        }

        // Object state

        private Row leftRow;
        private Row rightRow;
        private ValuesHolderRow leftSkipRow;
        private ValuesHolderRow rightSkipRow;
        private boolean leftSkipRowFixed;
        private boolean rightSkipRowFixed;
    }
}
