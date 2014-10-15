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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

public class CountIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(order, 11L, 1L, "ori")
        };
        use(dbRows);
    }

    // Count tests

    @Test
    public void testCustomerCid()
    {
        Operator plan = count_Default(groupScan_Default(coi),
                                              customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType resultRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomers()
    {
        Operator plan = count_TableStatus(customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType resultRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
        writeRows(createNewRow(customer, 5L, "matrix"));
        expected = new Row[]{
            row(resultRowType, 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCount_DefaultCursor()
    {
        Operator plan =
            count_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    @Test
    public void testCount_TableStatusCursor()
    {
        Operator plan = count_TableStatus(customerRowType);
        final RowType resultRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(resultRowType, 3L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
