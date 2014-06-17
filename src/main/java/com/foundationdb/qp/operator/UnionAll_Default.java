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
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 <h1>Overview</h1>

 UnionAll_Default generates an output stream containing all the rows of both input streams. There are no
 guarantees on output order, and duplicates are not eliminated.

 <h1>Arguments</h1>

 <li><b>Operator input1:</b> Source of first input stream. 
 <li><b>RowType input1Type:</b> Type of rows in first input stream. 
 <li><b>Operator input2:</b> Source of second input stream. 
 <li><b>RowType input2Type:</b> Type of rows in second input stream. 
 <li><b>boolean openBoth:</b> Whether to open both input cursors at the same time rather than as needed. 

 <h1>Behavior</h1>

 The output from UnionAll_Default is formed by concatenating the first and second input streams.

 <h1>Output</h1>

 Rows of the first input stream followed by rows of the second input stream.

 <h1>Assumptions</h1>

 input1Type and input2Type are union-compatible. This means input1Type == input2Type or they have the same
 number of fields, and that corresponding field types match.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 None.

 */

final class UnionAll_Default extends SetOperatorBase {

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }

    UnionAll_Default(Operator input1, RowType input1Type, Operator input2, RowType input2Type, boolean openBoth) {
        super(input1, input1Type, input2, input2Type, "Union");
        this.openBoth = openBoth;
    }


    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: UnionAll_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: UnionAll_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(UnionAll_Default.class);

    // Object state

    private final boolean openBoth;

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
        att.put(Label.PIPELINE, PrimitiveExplainer.getInstance(openBoth));
        return new CompoundExplainer(Type.UNION, att);
    }

    private class Execution extends OperatorCursor {

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                idle = false;
                if (openBoth) {
                    for (int i = 0; i < cursors.length; i++) {
                        cursors[i].open();
                    }
                }
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row next;
                if (currentCursor == null) {
                    next = nextCursorFirstRow();
                } else {
                    next = currentCursor.next();
                    if (next == null) {
                        currentCursor.close();
                        next = nextCursorFirstRow();
                    }
                }
                if (next == null) {
                    close();
                    idle = true;
                } else {
                    next = wrapped(next);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("UnionAll_Default: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            if (currentCursor != null) {
                currentCursor.close();
                currentCursor = null;
            }
            if (openBoth) {
                while (++inputOperatorsIndex < getInputSize()) {
                    cursors[inputOperatorsIndex].close();
                }
            }
            inputOperatorsIndex = -1;
            currentInputRowType = null;
            idle = true;
        }

        @Override
        public void destroy() {
            close();
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            destroyed = true;
        }

        @Override
        public boolean isIdle() {
            return !destroyed && idle;
        }

        @Override
        public boolean isActive() {
            return !destroyed && !idle;
        }

        @Override
        public boolean isDestroyed() {
            return destroyed;
        }

        @Override
        public void openBindings() {
            bindingsCursor.openBindings();
            for (int i = 0; i < cursors.length; i++) {
                cursors[i].openBindings();
            }//recursivly open bidings
        }

        @Override
        public QueryBindings nextBindings() {
            QueryBindings bindings = bindingsCursor.nextBindings();
            for (int i = 0; i < cursors.length; i++) {
                QueryBindings other = cursors[i].nextBindings();
                assert (bindings == other);
            }
            return bindings;
        }

        @Override
        public void closeBindings() {
            bindingsCursor.closeBindings();
            for (int i = 0; i < cursors.length; i++) {
                cursors[i].closeBindings();
            }
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            for (int i = 0; i < cursors.length; i++) {
                cursors[i].cancelBindings(bindings);
            }
            bindingsCursor.cancelBindings(bindings);
        }

        private Execution(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context);
            MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
            this.bindingsCursor = multiple;
            cursors = new Cursor[getInputSize()];
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = operator(i).cursor(context, multiple.newCursor());
            }
        }

        /**
         * Opens as many cursors as it takes to get one that returns a first row. Whichever is the first cursor
         * to return a non-null row, that cursor is saved as this.currentCursor. If no cursors remain that have
         * a next row, returns null.
         *
         * @return the first row of the next cursor that has a non-null row, or null if no such cursors remain
         */
        private Row nextCursorFirstRow() {
            while (++inputOperatorsIndex < getInputSize()) {
                Cursor nextCursor = cursors[inputOperatorsIndex];
                if (!openBoth) {
                    nextCursor.open();
                }
                Row nextRow = nextCursor.next();
                if (nextRow == null) {
                    nextCursor.close();
                } else {
                    currentCursor = nextCursor;
                    this.currentInputRowType = inputRowType(inputOperatorsIndex);
                    return nextRow;
                }
            }
            return null;
        }

        private Row wrapped(Row inputRow) {
            assert inputRow != null;
            if (!inputRow.rowType().equals(currentInputRowType)) {
                throw new WrongRowTypeException(inputRow, currentInputRowType);
            }
            if (currentInputRowType == rowType()) {
                return inputRow;
            }
            OverlayingRow row = new OverlayingRow(inputRow, rowType());
            return row;
        }

        private final QueryBindingsCursor bindingsCursor;
        private int inputOperatorsIndex = -1; // right before the first operator
        private Cursor[] cursors;
        private Cursor currentCursor;
        private RowType currentInputRowType;
        private boolean idle = true;
        private boolean destroyed = false;
    }

    static class WrongRowTypeException extends AkibanInternalException {
        public WrongRowTypeException(Row row, RowType expected) {
            super(row + ": expected row type " + expected + " but was " + row.rowType());
        }
    }
}


