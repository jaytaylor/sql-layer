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

import com.akiban.qp.row.PValuesRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.CountOperatorExplainer;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Count_Default counts the number of rows of a specified RowType.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType countType:</b> Type of rows to be counted.

 </ul>


 <h1>Behavior</h1>

 The input rows whose type matches the countType are counted.

 <h1>Output</h1>

 A single row containing the row count (type long).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 This operator keeps no rows in memory.

 */

class Count_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), countType);
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
        return resultType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(resultType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Count_Default interface

    public Count_Default(Operator inputOperator, RowType countType, boolean usePValues)
    {
        ArgumentValidation.notNull("countType", countType);
        this.inputOperator = inputOperator;
        this.countType = countType;
        this.usePValues = usePValues;
        this.resultType = usePValues
            ? countType.schema().newValuesType(MNumeric.BIGINT.instance(false))
            : countType.schema().newValuesType(AkType.LONG);
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Count_Default.class);

    // Object state

    private final Operator inputOperator;
    private final RowType countType;
    private final ValuesRowType resultType;
    private final boolean usePValues;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new CountOperatorExplainer(getName(), countType, resultType, inputOperator, context);
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
                CursorLifecycle.checkIdle(this);
                input.open();
                count = 0;
                closed = false;
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
                checkQueryCancelation();
                Row row = null;
                while ((row == null) && !closed) {
                    row = input.next();
                    if (row == null) {
                        close();
                        row = usePValues
                                ? new PValuesRow(resultType, new PValue(MNumeric.BIGINT.instance(false), count))
                                : new ValuesRow(resultType, new Object[] { count });
                    } else if (row.rowType() == countType) {
                        row = null;
                        count++;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Count_Default: yield {}", row);
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
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                input.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
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
            return input.isDestroyed();
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

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private long count;
        private boolean closed = true;
    }
}
