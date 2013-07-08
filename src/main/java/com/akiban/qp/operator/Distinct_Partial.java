/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor), usePValue);
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
    private static final Logger LOG = LoggerFactory.getLogger(Distinct_Partial.class);

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
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                checkQueryCancelation();
                Row row;
                while ((row = input.next()) != null) {
                    assert row.rowType() == distinctType : row;
                    boolean isDistinct = (currentValues == null) ? isDistinctP(row) : isDistinct(row);
                    if (isDistinct) // TODO inline this var once legacy types are gone
                        break;
                }
                if (row == null) {
                    close();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Distinct_Partial: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
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

        @Override
        public void openBindings() {
            input.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            return input.nextBindings();
        }

        @Override
        public void closeBindings() {
            input.closeBindings();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input, boolean usePValue)
        {
            super(context);
            this.input = input;

            nfields = distinctType.nFields();
            if (!usePValue) {
                currentValues = new ValueHolder[nfields];
                currentPValues = null;
            }
            else {
                currentValues = null;
                currentPValues = new PValue[nfields];
                for (int i = 0; i < nfields; ++i) {
                    currentPValues[i] = new PValue(distinctType.typeInstanceAt(i));
                }
            }
        }

        private boolean isDistinctP(Row inputRow) {
            if ((nvalid == 0) && currentRow.isEmpty()) {
                // Very first row.
                currentRow.hold(inputRow);
                return true;
            }
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow.isHolding();
                    PValueTargets.copyFrom(currentRow.get().pvalue(i), currentPValues[i]);
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow.release();
                }
                PValueSource inputValue = inputRow.pvalue(i);
                if (!eqP(currentPValues[i], inputValue, rowType().typeInstanceAt(i))) {
                    PValueTargets.copyFrom(inputValue, currentPValues[i]);
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow.hold(inputRow);
                    return true;
                }
            }
            return false;
        }

        private boolean isDistinct(Row inputRow) 
        {
            if ((nvalid == 0) && currentRow.isEmpty()) {
                // Very first row.
                currentRow.hold(inputRow);
                return true;
            }
            ValueHolder inputValue = new ValueHolder();
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow.isHolding();
                    if (currentValues[i] == null)
                        currentValues[i] = new ValueHolder();
                    currentValues[i].copyFrom(currentRow.get().eval(i));
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow.release();
                }
                inputValue.copyFrom(inputRow.eval(i));
                if (!eq(i, currentValues[i], inputValue)) {
                    currentValues[i] = inputValue;
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow.hold(inputRow);
                    return true;
                }
            }
            return false;
        }

        private boolean eq(int field, ValueSource x, ValueSource y)
        {
            if (collators == null) {
                return currentValues[field].equals(y);
            } else {
                AkCollator collator = collators.get(field);
                if (collator == null) {
                    return currentValues[field].equals(y);
                } else {
                    return collator.compare(x, y) == 0;
                }
            }
        }

        private boolean eqP(PValueSource x, PValueSource y, TInstance tinst)
        {
            if (tinst.typeClass() instanceof TString) {
                AkCollator collator = ((TString)tinst.typeClass()).getCollator(tinst);
                if (collator != null) {
                    return collator.compare(x, y) == 0;
                }
            }
            return PValueSources.areEqual(x, y, tinst);
        }

        // Object state

        private final Cursor input;
        private final ShareHolder<Row> currentRow = new ShareHolder<>();
        private final int nfields;
        // currentValues contains copies of the first nvalid of currentRow's fields,
        // filled as needed.
        private int nvalid;
        private final ValueHolder[] currentValues;
        private final PValue[] currentPValues;
        private boolean idle = true;
        private boolean destroyed = false;
    }
}
