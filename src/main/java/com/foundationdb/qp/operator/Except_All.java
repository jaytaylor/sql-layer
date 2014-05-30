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
 * Created by jerett on 5/29/14.
 */
final class Except_All extends SetOperatorBase {

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }

    public Except_All(Operator left,
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
        // The following assumptions will be relaxed when this operator is generalized to support inputs from different
        // indexes.
        ArgumentValidation.isEQ("leftOrderingFields", leftOrderingFields, "rightOrderingFields", rightOrderingFields);
        // Setup for row comparisons
        this.fixedFields = rowType().nFields() - leftOrderingFields;
        this.fieldsToCompare = ascending.length;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        this.removeDuplicates = removeDuplicates;
        // TODO (in Execution): Check that ascending bits are consistent with IndexCursor directions.
    }


    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: ExceptAll_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: ExceptAll_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(UnionAll_Default.class);

    // Object state

    private final int fixedFields;
    private final int fieldsToCompare;
    private final boolean[] ascending;
    private final boolean removeDuplicates;


    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes att = new Attributes();
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        att.put(Label.SET_OPTION, PrimitiveExplainer.getInstance("ALL"));
        for (Operator op : getInputOperators())
            att.put(Label.INPUT_OPERATOR, op.getExplainer(context));
        for (RowType type : getInputTypes())
            att.put(Label.INPUT_TYPE, type.getExplainer(context));
        att.put(Label.OUTPUT_TYPE, rowType().getExplainer(context));
        if(!removeDuplicates)
            att.put(Label.SET_OPTION, PrimitiveExplainer.getInstance("ALL"));
        return new CompoundExplainer(Type.EXCEPT, att);
    }

    private class Execution extends OperatorCursor {
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                leftInput.open();
                rightInput.open();
                nextRightRow();
                nextLeftRow();
                previousRow = null;
                closed = false;
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
                Row next;
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
                    }//right stream is less so grab next from rightRow
                    else if(c < 0){
                        if(removeDuplicates && compareToPrevious() == 0){
                            nextLeftRow();
                        } else {
                            found = true;
                        }
                    }//could just be an else
                }
                if(removeDuplicates && compareToPrevious() == 0){
                    nextLeftRow();
                    return next();
                }
                next = leftRow;
                nextLeftRow();
                if (LOG_EXECUTION) {
                    LOG.debug("Except_All: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED)
                    TAP_NEXT.out();
            }
        }

        private void nextLeftRow()
        {
            Row row = leftInput.next();
            previousRow = leftRow;
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
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
            this.bindingsCursor = multiple;
            this.leftInput = left().cursor(context, multiple.newCursor());
            this.rightInput = right().cursor(context, multiple.newCursor());
        }


        private int compareToPrevious() {
            if (previousRow == null || leftRow == null) {
                return 1;
            }
            return leftRow.compareTo(previousRow, fixedFields, fixedFields, fieldsToCompare);
        }

        private int compareRows(){
            int c;
            assert !closed;
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
                assert fieldThatDiffers < ascending.length;
                if (!ascending[fieldThatDiffers]) {
                    c = -c;
                }
            }
            return c;
        }



        private boolean closed = true;
        private final QueryBindingsCursor bindingsCursor;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private Row leftRow;
        private Row rightRow;
        private Row previousRow;
    }
}

