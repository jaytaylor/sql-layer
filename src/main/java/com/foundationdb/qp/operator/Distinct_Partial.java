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
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.DistinctExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
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
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
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

    public Distinct_Partial(Operator inputOperator, RowType distinctType, List<AkCollator> collators)
    {
        ArgumentValidation.notNull("distinctType", distinctType);
        this.inputOperator = inputOperator;
        this.distinctType = distinctType;
        this.collators = collators;
    }

    // Class state
    
    private final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial open");
    private final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Distinct_Partial next");
    private static final Logger LOG = LoggerFactory.getLogger(Distinct_Partial.class);

    // Object state

    private final Operator inputOperator;
    private final RowType distinctType;
    private final List<AkCollator> collators;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new DistinctExplainer(getName(), distinctType, inputOperator, context);
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                nvalid = 0;
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
                    if (isDistinctP(row)) break;
                }
                if (row == null) {
                    setIdle();
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
            super.close();
            currentRow = null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);

            nfields = distinctType.nFields();
            currentValues = new Value[nfields];
            for (int i = 0; i < nfields; ++i) {
                currentValues[i] = new Value(distinctType.typeAt(i));
            }
        }

        private boolean isDistinctP(Row inputRow) {
            if ((nvalid == 0) && currentRow == null) {
                // Very first row.
                currentRow = inputRow;
                return true;
            }
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow != null;
                    ValueTargets.copyFrom(currentRow.value(i), currentValues[i]);
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow = null;
                }
                ValueSource inputValue = inputRow.value(i);
                if (!eqP(currentValues[i], inputValue, rowType().typeAt(i))) {
                    ValueTargets.copyFrom(inputValue, currentValues[i]);
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow = inputRow;
                    return true;
                }
            }
            return false;
        }

        private boolean eqP(ValueSource x, ValueSource y, TInstance type)
        {
            if (type.typeClass() instanceof TString) {
                AkCollator collator = TString.getCollator(type);
                if (collator != null) {
                    return collator.compare(x, y) == 0;
                }
            }
            return ValueSources.areEqual(x, y, type);
        }

        // Object state

        private Row currentRow;
        private final int nfields;
        // currentValues contains copies of the first nvalid of currentRow's fields,
        // filled as needed.
        private int nvalid;
        private final Value[] currentValues;
    }
}
