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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 <h1>Overview</h1>

 UnionAll_Default generates an output stream containing all the rows of both input streams. There are no
 guarantees on output order, and duplicates are not eliminated.

 <h1>Arguments</h1>

 <li><b>Operator input1:</b> Source of first input stream. 
 <li><b>RowType input1Type:</b> Type of rows in first input stream. 
 <li><b>Operator input2:</b> Source of second input stream. 
 <li><b>RowType input2Type:</b> Type of rows in second input stream. 

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

final class UnionAll_Default extends Operator {
    @Override
    public List<Operator> getInputOperators() {
        return Collections.unmodifiableList(inputs);
    }

    @Override
    public RowType rowType() {
        return outputRowType;
    }

    @Override
    public String describePlan() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, end = inputs.size(); i < end; ++i) {
            Operator input = inputs.get(i);
            sb.append(input);
            if (i + 1 < end)
                sb.append(Strings.nl()).append("UNION ALL").append(Strings.nl());
        }
        return sb.toString();
    }

    @Override
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, inputs, inputTypes, outputRowType);
    }

    UnionAll_Default(Operator input1, RowType input1Type, Operator input2, RowType input2Type) {
        ArgumentValidation.notNull("first input", input1);
        ArgumentValidation.notNull("second input", input2);
        this.outputRowType = rowType(input1Type, input2Type);
        this.inputs = Arrays.asList(input1, input2);
        this.inputTypes = Arrays.asList(input1Type, input2Type);
    }

    // for use in this package (in ctor and unit tests)

    static RowType rowType(RowType rowType1, RowType rowType2) {
        if (rowType1 == rowType2)
            return rowType1;
        if (rowType1.nFields() != rowType2.nFields())
            throw notSameShape(rowType1, rowType2);
        AkType[] types = new AkType[rowType1.nFields()];
        for(int i=0; i<types.length; ++i) {
            AkType akType1 = rowType1.typeAt(i);
            AkType akType2 = rowType2.typeAt(i);
            if (akType1.equals(akType2))
                types[i] = akType1;
            else if (akType1 == AkType.NULL)
                types[i] = akType2;
            else if (akType2 == AkType.NULL)
                types[i] = akType1;
            else
                throw notSameShape(rowType1, rowType2);
        }
        return rowType1.schema().newValuesType(types);
    }

    private static IllegalArgumentException notSameShape(RowType rt1, RowType rt2) {
        return new IllegalArgumentException(String.format("RowTypes not of same shape: %s (%s), %s (%s)",
                rt1, akTypesOf(rt1),
                rt2, akTypesOf(rt2)
        ));
    }

    private static String akTypesOf(RowType rt) {
        AkType[] result = new AkType[rt.nFields()];
        for (int i=0; i < result.length; ++i) {
            result[i] = rt.typeAt(i);
        }
        return Arrays.toString(result);
    }
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: UnionAll_Default open"); 
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: UnionAll_Default next"); 
    
    // Object state

    private final List<? extends Operator> inputs;
    private final List<? extends RowType> inputTypes;
    private final RowType outputRowType;

    private static final class Execution implements Cursor {


        @Override
        public void open() {
            TAP_OPEN.in();
            TAP_OPEN.out();
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                Row outputRow;
                if (currentCursor == null) {
                    outputRow = nextCursorFirstRow();
                }
                else {
                    outputRow = currentCursor.next();
                    if (outputRow == null) {
                        currentCursor.close();
                        outputRow = nextCursorFirstRow();
                    }
                }
                if (outputRow == null) {
                    close();
                    return null;
                }
                return wrapped(outputRow);
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close() {
            inputOperatorsIndex = -1;
            if (currentCursor != null)
                currentCursor.close();
            else
                currentCursor = null;
            currentInputRowType = null;
            rowHolder.release();
        }

        private Execution(QueryContext context,
                          List<? extends Operator> inputOperators,
                          List<? extends RowType> inputRowTypes,
                          RowType outputType)
        {
            this.context = context;
            this.inputOperators = inputOperators;
            this.inputRowTypes = inputRowTypes;
            this.outputRowType = outputType;
            assert this.inputOperators.size() == this.inputRowTypes.size()
                    : this.inputOperators + ".size() != " + this.inputRowTypes.size() + ".size()";
        }

        /**
         * Opens as many cursors as it takes to get one that returns a first row. Whichever is the first cursor
         * to return a non-null row, that cursor is saved as this.currentCursor. If no cursors remain that have
         * a next row, returns null.
         * @return the first row of the next cursor that has a non-null row, or null if no such cursors remain
         */
        private Row nextCursorFirstRow() {
            while (++inputOperatorsIndex < inputOperators.size()) {
                Cursor nextCursor = inputOperators.get(inputOperatorsIndex).cursor(context);
                nextCursor.open();
                Row nextRow = nextCursor.next();
                if (nextRow == null) {
                    nextCursor.close();
                }
                else {
                    currentCursor = nextCursor;
                    this.currentInputRowType = inputRowTypes.get(inputOperatorsIndex);
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
            assert inputRow.rowType().equals(currentInputRowType) : inputRow.rowType() + " != " + currentInputRowType;
            MasqueradingRow row;
            if (rowHolder.isEmpty() || rowHolder.isShared()) {
                row = new MasqueradingRow(outputRowType, inputRow);
                rowHolder.hold(row);
            }
            else {
                row = rowHolder.get();
                rowHolder.release();
                row.setRow(inputRow);
            }
            rowHolder.hold(row);
            return row;
        }

        private final QueryContext context;
        private final List<? extends Operator> inputOperators;
        private final List<? extends RowType> inputRowTypes;
        private final RowType outputRowType;
        private final ShareHolder<MasqueradingRow> rowHolder = new ShareHolder<MasqueradingRow>();
        private int inputOperatorsIndex = -1; // right before the first operator
        private Cursor currentCursor;
        private RowType currentInputRowType;
    }

    static class WrongRowTypeException extends AkibanInternalException {
        public WrongRowTypeException(Row row, RowType expected) {
            super(row + ": expected row type " + expected + " but was " + row.rowType());
        }
    }

    private static class MasqueradingRow implements Row {

        @Override
        public RowType rowType() {
            return rowType; // Note! Not a delegate
        }

        @Override
        public HKey hKey() {
            return delegate.hKey();
        }

        @Override
        public boolean ancestorOf(RowBase that) {
            return delegate.ancestorOf(that);
        }

        @Override
        public boolean containsRealRowOf(UserTable userTable) {
            return delegate.containsRealRowOf(userTable);
        }

        @Override
        public int runId() {
            return delegate.runId();
        }

        @Override
        public void runId(int runId) {
            delegate.runId(runId);
        }

        @Override
        public Row subRow(RowType subRowType) {
            return delegate.subRow(subRowType);
        }

        @Override
        public ValueSource eval(int index) {
            return delegate.eval(index);
        }

        /**
         * @see #isShared()
         */
        @Override
        public void acquire() {
            ++shares;
            delegate.acquire();
        }

        /**
         * Returns this MasqueradingRow, or its delegate, are shared. It's not enough to only delegate this method
         * (and the acquire/release methods that go along with it), because if the delegate row is never shared (as
         * happens with an immutable row, for instance), we still want to mark this MasqueradingRow as shared.
         * Without that, the Execution will reuse this wrapper -- by giving it a new delegate -- which will break
         * the sharing contract.
         * @return whether this row is shared
         */
        @Override
        public boolean isShared() {
            return (shares > 1) || delegate.isShared();
        }

        /**
         * @see #isShared()
         */
        @Override
        public void release() {
            assert shares > 0 : shares;
            delegate.release();
            --shares;
        }

        @Override
        public String toString() {
            return delegate.toString() + " of type " + rowType;
        }

        void setRow(Row row) {
            assert shares == 0;
            this.delegate = row;
        }

        private MasqueradingRow(RowType rowType, Row wrapped) {
            this.rowType = rowType;
            this.delegate = wrapped;
            shares = 0;
        }

        private Row delegate;
        private final RowType rowType;
        private int shares;
    }
}
