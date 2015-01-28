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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 ExceptOperator outputs all rows from the left input stream that are not comparably equal to rows in the right input stream,
 This operator requires that both input streams are in sorted order.  Multiple instance of the same row in the left stream
 will only be removed if there are the same number of equal instances in the right stream or Duplicates are suppressed.
 Duplicates can be suppressed using the removeDuplicates bool in the constructor making it act as an Except Distinct

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
 <li><b>boolean removeDuplicates<:/b>This boolean can be used to suppress duplicates from being output</li>

 <h1>Behavior</h1>

 The left stream is iterated over, The row is output if there is not a row in the right stream that is considered equal
 to it given the fields to compare

 <h1>Output</h1>

 All rows form the left stream that do not have equal row in right stream

 <h1>Assumptions</h1>

 Each input stream is ordered by its ordering columns, as determined by <tt>leftOrderingFields</tt>
 and <tt>rightOrderingFields</tt>.

 For now: leftRowType == inputRowType and leftOrderingFields == rightOrderingFields.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 Three input rows, one from each stream and the previous row from the left stream if removeDuplicates is set.

 */

final class Except_Ordered extends SetOperatorBase {

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }

    public Except_Ordered(Operator left,
                          Operator right,
                          RowType leftRowType,
                          RowType rightRowType,
                          int leftOrderingFields,
                          int rightOrderingFields,
                          boolean[] ascending,
                          boolean removeDuplicates)
    {
        super (left, leftRowType, right, rightRowType, "Except");
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("ascending.length()", ascending.length, 0);
        ArgumentValidation.isLTE("ascending.length()", ascending.length, min(leftOrderingFields, rightOrderingFields));
        ArgumentValidation.isEQ("leftOrderingFields", leftOrderingFields, "rightOrderingFields", rightOrderingFields);
        // Setup for row comparisons
        this.fixedFields = rowType().nFields() - leftOrderingFields;
        this.fieldsToCompare = ascending.length;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        this.removeDuplicates = removeDuplicates;
        // TODO (in Execution): Check that ascending bits are consistent with IndexCursor directions.
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Except_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Except_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Except_Ordered.class);

    // Object state

    private final int fixedFields;
    private final int fieldsToCompare;
    private final boolean[] ascending;
    private final boolean removeDuplicates;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes att = new Attributes();
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        att.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(fixedFields));
        att.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(fieldsToCompare));
        for (Operator op : getInputOperators())
            att.put(Label.INPUT_OPERATOR, op.getExplainer(context));
        for (RowType type : getInputTypes())
            att.put(Label.INPUT_TYPE, type.getExplainer(context));
        att.put(Label.OUTPUT_TYPE, rowType().getExplainer(context));
        if(!removeDuplicates)
            att.put(Label.SET_OPTION, PrimitiveExplainer.getInstance("ALL"));
        return new CompoundExplainer(Type.ORDERED, att);
    }

    private class Execution extends MultiChainedCursor { 
        
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                nextRightRow();
                nextLeftRow();
                previousRow = null;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next(){
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row next = null;
                boolean found = false;
                while(!found && leftRow != null && rightRow != null)
                {
                    int c = compareRows();
                    if(c == 0){
                        nextRightRow();
                        nextLeftRow();
                    }//match
                    else if(c > 0){
                        nextRightRow();
                    }
                    else if(c < 0){
                        //next = leftRow;
                        found = true;
                    }
                }
                next = leftRow;
                if(removeDuplicates && compareToPrevious() == 0){
                    nextLeftRow();
                    return next();
                }
                if(leftRow != null) {
                    nextLeftRow();
                }
                if(next != null) {
                    next = wrapped(next);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Except_Ordered: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED)
                    TAP_NEXT.out();
            }
        }

        private void nextLeftRow() {
            Row row = leftInput.next();
            previousRow = leftRow;
            leftRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("Except_Ordered: left {}", row);
            }
        }

        private void nextRightRow() {
            Row row = rightInput.next();
            rightRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("Except_Ordered: right {}", row);
            }
        }

        @Override
        public void close() {
            super.close();
            leftRow = null;
            rightRow = null;
        }

        @Override 
        protected Operator left() {
            return Except_Ordered.this.left();
        }
        
        @Override
        protected Operator right() {
            return Except_Ordered.this.right();
        }
        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context, bindingsCursor);
        }

        private int compareToPrevious() {
            if (previousRow == null || leftRow == null) {
                return 1;
            }
            return leftRow.compareTo(previousRow, fixedFields, fixedFields, fieldsToCompare);
        }

        private int compareRows(){
            int c;
            assert !isClosed();
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

        private int adjustComparison(int c) {
            if (c != 0) {
                int fieldThatDiffers = abs(c) - 1;
                assert fieldThatDiffers < ascending.length;
                if (!ascending[fieldThatDiffers]) {
                    c = -c;
                }
            }
            return c;
        }

        private Row wrapped(Row inputRow) {
            assert inputRow != null;
            if (!useOverlayRow()) {
                return inputRow;
            }
            OverlayingRow row = new OverlayingRow(inputRow, rowType());
            return row;
        }

        private Row leftRow;
        private Row rightRow;
        private Row previousRow;
    }
}
