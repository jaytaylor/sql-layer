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

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.DistinctExplainer;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.*;

/**

 <h1>Overview</h1>

 Distinct_Partial eliminates duplicate rows that are adjacent in the input stream.
 (A sufficient but not necessary condition for elimination of all duplicates is
 that the input is sorted by all columns.) 

 <h1>Arguments</h1>

 <ul>

 <li><b>Operator input:</b> the input operator

 <li><b>RowType distinctType:</b> Specifies the type of rows from the input stream.

 </ul>

 <h1>Behavior</h1>
 
 The RowType of each input row must match the specified distinctType.
 
 For each maximal subsequence of input rows that match in all columns, one row will be written to output. 

 <h1>Output</h1>

 A subset of the input rows, in which no two adjacent rows match in all columns.
 
 <h1>Assumptions</h1>

 The input type of every input row is the specified distinctType.

 <h1>Performance</h1>

 This operator performs no IO. For each row, there is a comparison of one or more column values to the columns
 of a stored row. An attempt is made to minimize the number of such comparisons, but it is possible to compare
 every column of every input row.

 <h1>Memory requirements</h1>

 No more than two rows at any time.

 */

class Distinct_Partial extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), distinctType);
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context), usePValue);
    }

    @Override
    public RowType rowType()
    {
        return distinctType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(distinctType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Distinct_Partial interface

    public Distinct_Partial(Operator inputOperator, RowType distinctType, List<AkCollator> collators, boolean usePValue)
    {
        ArgumentValidation.notNull("distinctType", distinctType);
        this.inputOperator = inputOperator;
        this.distinctType = distinctType;
        this.collators = collators;
        this.usePValue = usePValue;
    }

    // Class state
    
    private final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial open");
    private final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial next");
    
    // Object state

    private final Operator inputOperator;
    private final RowType distinctType;
    private final List<AkCollator> collators;
    private final boolean usePValue;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new DistinctExplainer(getName(), distinctType, inputOperator, context);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                input.open();
                nvalid = 0;
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                checkQueryCancelation();
                Row row;
                while ((row = input.next()) != null) {
                    assert row.rowType() == distinctType : row;
                    boolean isDistinct = isDistinct(row, valuesAdapter);
                    if (isDistinct) // TODO inline this var once legacy types are gone
                        break;
                }
                if (row == null) {
                    close();
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            if (!idle) {
                input.close();
                currentRow.release();
                idle = true;
            }
        }

        @Override
        public void destroy()
        {
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

        // Execution interface

        Execution(QueryContext context, Cursor input, boolean usePValue)
        {
            super(context);
            this.input = input;

            nfields = distinctType.nFields();

            if (!usePValue) {
                valuesAdapter = new OldValuesAdapter(nfields);
            }
            else {
                valuesAdapter = new NewValuesAdapter(nfields, distinctType);
            }
        }

        private <T> boolean isDistinct(Row inputRow, ValuesAdapter<T> valuesAdapter)
        {
            if ((nvalid == 0) && currentRow.isEmpty()) {
                // Very first row.
                currentRow.hold(inputRow);
                return true;
            }
            T inputValue = valuesAdapter.createRegister();
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow.isHolding();
                    valuesAdapter.copy(currentRow, i);
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow.release();
                }
                valuesAdapter.copy(inputRow, i, inputValue);
                if (!valuesAdapter.areEqual(i, inputValue)) {
                    valuesAdapter.copy(inputValue, i);
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow.hold(inputRow);
                    return true;
                }
            }
            return false;
        }

        // Object state

        private final Cursor input;
        private final ShareHolder<Row> currentRow = new ShareHolder<Row>();
        private final int nfields;
        // currentValues contains copies of the first nvalid of currentRow's fields,
        // filled as needed.
        private int nvalid;
        private final ValuesAdapter<?> valuesAdapter;
//        private final ValueHolder[] currentValues;
//        private final PValue[] currentPValues;
        private boolean idle = true;
        private boolean destroyed = false;
    }

    private interface ValuesAdapter<T> {
        void copy(ShareHolder<Row> currentRow, int i);
        void copy(Row inputRow, int i, T inputValue);
        T createRegister();
        void copy(T inputValue, int i);
        boolean areEqual(int field, T inputValue);
    }

    private class OldValuesAdapter implements ValuesAdapter<ValueHolder> {

        @Override
        public void copy(ShareHolder<Row> currentRow, int i) {
            if (values[i] == null)
                values[i] = new ValueHolder();
            values[i].copyFrom(currentRow.get().eval(i));
        }

        @Override
        public void copy(Row inputRow, int i, ValueHolder inputValue) {
            inputValue.copyFrom(inputRow.eval(i));
        }

        @Override
        public void copy(ValueHolder inputValue, int i) {
            values[i] = inputValue;
        }

        @Override
        public ValueHolder createRegister() {
            return new ValueHolder();
        }

        @Override
        public boolean areEqual(int field, ValueHolder inputValue) {
            if (collators == null) {
                return values[field].equals(inputValue);
            } else {
                AkCollator collator = collators.get(field);
                if (collator == null) {
                    return values[field].equals(inputValue);
                } else {
                    return collator.compare(values[field], inputValue) == 0;
                }
            }

        }

        public OldValuesAdapter(int nfields) {
            values = new ValueHolder[nfields];
        }

        private final ValueHolder[] values;
    }

    private class NewValuesAdapter implements ValuesAdapter<PValue> {

        @Override
        public void copy(ShareHolder<Row> currentRow, int i) {
            PValueTargets.copyFrom(currentRow.get().pvalue(i), values[i]);
        }

        @Override
        public void copy(Row inputRow, int i, PValue inputValue) {
            PValueSource inputSource = inputRow.pvalue(i);
            inputValue.underlying(inputSource.getUnderlyingType());
            PValueTargets.copyFrom(inputSource, inputValue);
        }

        @Override
        public PValue createRegister() {
            return new PValue();
        }

        @Override
        public void copy(PValue inputValue, int i) {
            values[i] = inputValue;
        }

        @Override
        public boolean areEqual(int field, PValue inputValue) {
            TInstance tinst = rowType().typeInstanceAt(field);
            if (tinst.typeClass() instanceof TString) {
                AkCollator collator = ((TString)tinst.typeClass()).getCollator(tinst);
                if (collator != null) {
                    return collator.compare(values[field], inputValue) == 0;
                }
            }
            return PValueSources.areEqual(values[field], inputValue, tinst);
        }

        public NewValuesAdapter(int nfields, RowType distinctType) {

            values = new PValue[nfields];
            for (int i = 0; i < nfields; ++i) {
                values[i] = new PValue(distinctType.typeInstanceAt(i).typeClass().underlyingType());
            }
        }

        private final PValue[] values;
    }
}
