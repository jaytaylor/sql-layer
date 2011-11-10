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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(adapter, inputs, inputTypes, outputRowType);
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

    private final List<? extends Operator> inputs;
    private final List<? extends RowType> inputTypes;
    private final RowType outputRowType;

    private static final class Execution implements Cursor {


        @Override
        public void open(Bindings bindings) {
            if (bindings == null)
                throw new IllegalArgumentException("bindings may not be null");
            this.bindings = bindings;
        }

        @Override
        public Row next() {
            if (bindings == null)
                throw new IllegalStateException("no bindings set");
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
        }

        @Override
        public void close() {
            this.bindings = null;
            if (currentCursor != null)
                currentCursor.close();
        }

        private Execution(StoreAdapter adapter,
                          List<? extends Operator> inputOperators,
                          List<? extends RowType> inputRowTypes,
                          RowType outputType)
        {
            this.adapter = adapter;
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
            assert bindings != null;
            while (++inputOperatorsIndex < inputOperators.size()) {
                Cursor nextCursor = inputOperators.get(inputOperatorsIndex).cursor(adapter);
                nextCursor.open(bindings);
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
            if (rowHolder.isEmpty() || rowHolder.isShared()) {
                MasqueradingRow row = new MasqueradingRow(outputRowType, inputRow);
                row.setRow(inputRow);
                rowHolder.hold(row);
            }
            else {
                rowHolder.get().setRow(inputRow);
            }
            return rowHolder.get();
        }

        private final StoreAdapter adapter;
        private final List<? extends Operator> inputOperators;
        private final List<? extends RowType> inputRowTypes;
        private final RowType outputRowType;
        private final ShareHolder<MasqueradingRow> rowHolder = new ShareHolder<MasqueradingRow>();
        private int inputOperatorsIndex = -1; // right before the first operator
        private Bindings bindings;
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
            delegate.release();
            --shares;
        }

        @Override
        public String toString() {
            return delegate.toString() + " of type " + rowType;
        }

        void setRow(Row row) {
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
