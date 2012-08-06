/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        return new Execution(context);
    }

    UnionAll_Default(Operator input1, RowType input1Type, Operator input2, RowType input2Type, boolean usePValues) {
        ArgumentValidation.notNull("first input", input1);
        ArgumentValidation.notNull("first input type", input1Type);
        ArgumentValidation.notNull("second input", input2);
        ArgumentValidation.notNull("second input type", input2Type);
        this.outputRowType = rowType(input1Type, input2Type, usePValues);
        this.inputs = Arrays.asList(input1, input2);
        this.inputTypes = Arrays.asList(input1Type, input2Type);
        ArgumentValidation.isEQ("inputs.size", inputs.size(), "inputTypes.size", inputTypes.size());
    }

    // for use in this package (in ctor and unit tests)

    static RowType rowType(RowType rowType1, RowType rowType2, boolean usePValues) {
        if (rowType1 == rowType2)
            return rowType1;
        if (rowType1.nFields() != rowType2.nFields())
            throw notSameShape(rowType1, rowType2);
        return usePValues ? rowTypeNew(rowType1, rowType2) : rowTypeOld(rowType1, rowType2);
    }

    private static RowType rowTypeNew(RowType rowType1, RowType rowType2) {
        TInstance[] types = new TInstance[rowType1.nFields()];
        for(int i=0; i<types.length; ++i) {
            TInstance tInst1 = rowType1.typeInstanceAt(i);
            TInstance tInst2 = rowType2.typeInstanceAt(i);
            if (tInst1.equals(tInst2))
                types[i] = tInst1;
            else
                throw notSameShape(rowType1, rowType2);
        }
        return rowType1.schema().newValuesType(types);
    }

    private static RowType rowTypeOld(RowType rowType1, RowType rowType2) {
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

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance("UNION ALL"));
        
        for (Operator op : inputs)
            att.put(Label.INPUT_OPERATOR, op.getExplainer(extraInfo));
        for (RowType type : inputTypes)
            att.put(Label.INPUT_TYPE, PrimitiveExplainer.getInstance(type));
       
        att.put(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(outputRowType));
        
        return new OperationExplainer(Type.UNION_ALL, att);
    }

    private class Execution extends OperatorExecutionBase implements Cursor {

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
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
                    idle = true;
                    return null;
                }
                return wrapped(outputRow);
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            inputOperatorsIndex = -1;
            if (currentCursor != null) {
                currentCursor.close();
                currentCursor = null;
            }
            currentInputRowType = null;
            rowHolder.release();
            idle = true;
        }

        @Override
        public void destroy()
        {
            close();
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            destroyed = true;
        }

        @Override
        public boolean isIdle()
        {
            return !destroyed && idle;
        }

        @Override
        public boolean isActive()
        {
            return !destroyed && !idle;
        }

        @Override
        public boolean isDestroyed()
        {
            return destroyed;
        }

        private Execution(QueryContext context)
        {
            super(context);
            cursors = new Cursor[inputs.size()];
        }

        /**
         * Opens as many cursors as it takes to get one that returns a first row. Whichever is the first cursor
         * to return a non-null row, that cursor is saved as this.currentCursor. If no cursors remain that have
         * a next row, returns null.
         * @return the first row of the next cursor that has a non-null row, or null if no such cursors remain
         */
        private Row nextCursorFirstRow() {
            while (++inputOperatorsIndex < inputs.size()) {
                Cursor nextCursor = cursor(inputOperatorsIndex);
                nextCursor.open();
                Row nextRow = nextCursor.next();
                if (nextRow == null) {
                    nextCursor.close();
                }
                else {
                    currentCursor = nextCursor;
                    this.currentInputRowType = inputTypes.get(inputOperatorsIndex);
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
            if (currentInputRowType == outputRowType) {
                return inputRow;
            }
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

        private Cursor cursor(int i)
        {
            if (cursors[i] == null) {
                cursors[i] = inputs.get(i).cursor(context);
            }
            return cursors[i];
        }

        private final ShareHolder<MasqueradingRow> rowHolder = new ShareHolder<MasqueradingRow>();
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

    private static class MasqueradingRow implements Row {

        @Override
        public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
        {
            return delegate.compareTo(row, leftStartIndex, rightStartIndex, fieldCount);
        }

        @Override
        public RowType rowType() {
            return rowType; // Note! Not a delegate
        }

        @Override
        public HKey hKey() {
            return delegate.hKey();
        }

        @Override
        public HKey ancestorHKey(UserTable table)
        {
            return delegate.ancestorHKey(table);
        }

        @Override
        public boolean ancestorOf(RowBase that) {
            return delegate.ancestorOf(that);
        }

        @Override
        public boolean containsRealRowOf(UserTable userTable) {
            throw new UnsupportedOperationException(getClass().toString());
        }

        @Override
        public Row subRow(RowType subRowType) {
            return delegate.subRow(subRowType);
        }

        @Override
        public ValueSource eval(int index) {
            return delegate.eval(index);
        }

        @Override
        public PValueSource pvalue(int index) {
            return delegate.pvalue(index);
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
