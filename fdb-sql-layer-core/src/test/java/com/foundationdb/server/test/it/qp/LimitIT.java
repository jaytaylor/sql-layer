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
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.NegativeLimitException;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

public class LimitIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(customer, 5L, "matrix"),
            createNewRow(customer, 6L, "sigma"),
            createNewRow(customer, 7L, "crv"),
        };
        use(dbRows);
    }

    // Limit tests

    @Test
    public void testLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              3);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkip()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, Integer.MAX_VALUE, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 6L, "sigma"),
            row(customerRowType, 7L, "crv"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipAndLimit()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              2, false, 2, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSkipExhausted()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              10, false, 1, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLimitFromBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), 2));
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(customerRowType, 2L, "foundation"),
        };
        compareRows(expected, cursor);
    }

    @Test(expected = NegativeLimitException.class)
    public void testLimitFromBadBinding()
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                                              0, false, 0, true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), -1));
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    @Test(expected=NegativeLimitException.class)
    public void testOffsetFromBadBinding () 
    {
        Operator plan = limit_Default(groupScan_Default(coi),
                0, true, 0, false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MNumeric.INT.instance(false), -1));
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan = limit_Default(groupScan_Default(coi), 3);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 4L, "highland"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
