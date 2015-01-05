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

import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.CountOperatorExplainer;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**

 <h1>Overview</h1>

 Count_TableStatus returns the row count for a given RowType.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType tableType:</b> RowType of the table whose count is to be returned.

 </ul>


 <h1>Behavior</h1>

 The count of rows of the specified table is read out of the table's TableStatus.

 <h1>Output</h1>

 A single row containing the row count (type long).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 This operator keeps no rows in memory.

 */

class Count_TableStatus extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), tableType);
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    @Override
    public RowType rowType()
    {
        return resultType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        derivedTypes.add(resultType);
    }

    // Count_TableStatus interface

    public Count_TableStatus(RowType tableType)
    {
        ArgumentValidation.notNull("tableType", tableType);
        ArgumentValidation.isTrue("tableType instanceof TableRowType",
                                  tableType instanceof TableRowType);
        this.tableType = tableType;
        this.resultType = tableType.schema().newValuesType(MNumeric.BIGINT.instance(false));
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Count_TableStatus open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Count_TableStatus next");
    private static final Logger LOG = LoggerFactory.getLogger(Count_TableStatus.class);

    // Object state

    private final RowType tableType;
    private final ValuesRowType resultType;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new CountOperatorExplainer(getName(), tableType, resultType, null, context);
    }

    // Inner classes

    private class Execution extends LeafCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
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
                Row output;
                checkQueryCancelation();
                if (isActive()) {
                    long rowCount = adapter().rowCount(adapter().getSession(), tableType);
                    setIdle();
                    output = new ValuesHolderRow(resultType, new Value(MNumeric.BIGINT.instance(false), rowCount));
                }
                else {
                    output = null;
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Count_TableStatus: yield {}", output);
                }
                return output;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, bindingsCursor);
        }

        // Object state
    }
}
