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

package com.akiban.server.test.it.qp;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class CountIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
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
        Cursor cursor = cursor(plan, queryContext);
        RowType resultRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomers()
    {
        Operator plan = count_TableStatus(customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowType resultRowType = plan.rowType();
        RowBase[] expected = new RowBase[]{
            row(resultRowType, 3L)
        };
        compareRows(expected, cursor);
        writeRows(createNewRow(customer, 5L, "matrix"));
        expected = new RowBase[]{
            row(resultRowType, 4L)
        };
        compareRows(expected, cursor);
    }

}
