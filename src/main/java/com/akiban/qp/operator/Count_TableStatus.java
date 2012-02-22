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

import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.std.CountOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;

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
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
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
        ArgumentValidation.isTrue("tableType instanceof UserTableRowType",
                                  tableType instanceof UserTableRowType);
        this.tableType = tableType;
        this.resultType = tableType.schema().newValuesType(AkType.LONG);
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Count_TableStatus open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Count_TableStatus next");
    
    // Object state

    private final RowType tableType;
    private final ValuesRowType resultType;

    @Override
    public Explainer getExplainer()
    {
        return new CountOperatorExplainer("Count_TableStatus", tableType, resultType, null);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            pending = true;
            TAP_OPEN.out();
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                checkQueryCancelation();
                if (pending) {
                    long rowCount = adapter().rowCount(tableType);
                    close();
                    return new ValuesRow(resultType, new Object[] { rowCount });
                }
                else {
                    return null;
                }
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            pending = false;
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
        }

        // Object state

        private boolean pending;
    }
}
