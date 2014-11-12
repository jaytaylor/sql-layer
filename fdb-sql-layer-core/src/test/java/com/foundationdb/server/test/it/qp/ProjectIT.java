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
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.server.test.ExpressionGenerators.field;
import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.JoinType.*;

public class ProjectIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        Row[] dbWithOrphans = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(customer, 4L, "highland"),
            row(order, 11L, 1L, "ori"),
            row(order, 12L, 1L, "david"),
            row(order, 21L, 2L, "tom"),
            row(order, 22L, 2L, "jack"),
            row(order, 31L, 3L, "peter"),
            row(item, 111L, 11L),
            row(item, 112L, 11L),
            row(item, 121L, 12L),
            row(item, 122L, 12L),
            row(item, 211L, 21L),
            row(item, 212L, 21L),
            row(item, 221L, 22L),
            row(item, 222L, 22L),
            // orphans
            row(item, 311L, 31L),
            row(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullRowType()
    {
        project_DefaultTest(groupScan_Default(coi), null, Arrays.asList(field(customerRowType, 0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullProjections()
    {
        project_DefaultTest(groupScan_Default(coi), customerRowType, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyProjections()
    {
        project_DefaultTest(groupScan_Default(coi), customerRowType, Collections.<ExpressionGenerator>emptyList());
    }

    // Projection tests

    @Test
    public void testCustomerCid()
    {
        Operator plan = project_DefaultTest(groupScan_Default(coi),
                                                customerRowType,
                                                Arrays.asList(field(customerRowType, 0)));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType projectedRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(projectedRowType, 1L),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(projectedRowType, 2L),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(projectedRowType, 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testReverseCustomerColumns()
    {
        Operator plan = project_DefaultTest(groupScan_Default(coi),
                                                customerRowType,
                                                Arrays.asList(field(customerRowType, 1), field(customerRowType, 0)));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType projectedRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(projectedRowType, "northbridge", 1L),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(projectedRowType, "foundation", 2L),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(projectedRowType, "highland", 4L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProjectOfFlatten()
    {
        // Tests projection of null too
        Operator flattenCO = flatten_HKeyOrdered(groupScan_Default(coi),
                                                         customerRowType,
                                                         orderRowType,
                                                         FULL_JOIN);
        RowType coType = flattenCO.rowType();
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO,
                                                          coType,
                                                          itemRowType,
                                                          FULL_JOIN);
        RowType coiType = flattenCOI.rowType();
        Operator plan =
            project_DefaultTest(flattenCOI,
                            coiType,
                            Arrays.asList(
                                field(coiType, 1), // customer name
                                field(coiType, 4), // salesman
                                field(coiType, 5))); // iid
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        RowType projectedRowType = plan.rowType();
        Row[] expected = new Row[]{
            row(projectedRowType, "northbridge", "ori", 111L),
            row(projectedRowType, "northbridge", "ori", 112L),
            row(projectedRowType, "northbridge", "david", 121L),
            row(projectedRowType, "northbridge", "david", 122L),
            row(projectedRowType, "foundation", "tom", 211L),
            row(projectedRowType, "foundation", "tom", 212L),
            row(projectedRowType, "foundation", "jack", 221L),
            row(projectedRowType, "foundation", "jack", 222L),
            row(projectedRowType, null, "peter", 311L),
            row(projectedRowType, null, "peter", 312L),
            row(projectedRowType, "highland", null, null)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        // Tests projection of null too
        Operator flattenCO = flatten_HKeyOrdered(groupScan_Default(coi),
                                                         customerRowType,
                                                         orderRowType,
                                                         FULL_JOIN);
        RowType coType = flattenCO.rowType();
        Operator flattenCOI = flatten_HKeyOrdered(flattenCO,
                                                          coType,
                                                          itemRowType,
                                                          FULL_JOIN);
        RowType coiType = flattenCOI.rowType();
        Operator plan =
            project_DefaultTest(flattenCOI,
                            coiType,
                            Arrays.asList(
                                field(coiType, 1), // customer name
                                field(coiType, 4), // salesman
                                field(coiType, 5))); // iid
        final RowType projectedRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(projectedRowType, "northbridge", "ori", 111L),
                    row(projectedRowType, "northbridge", "ori", 112L),
                    row(projectedRowType, "northbridge", "david", 121L),
                    row(projectedRowType, "northbridge", "david", 122L),
                    row(projectedRowType, "foundation", "tom", 211L),
                    row(projectedRowType, "foundation", "tom", 212L),
                    row(projectedRowType, "foundation", "jack", 221L),
                    row(projectedRowType, "foundation", "jack", 222L),
                    row(projectedRowType, null, "peter", 311L),
                    row(projectedRowType, null, "peter", 312L),
                    row(projectedRowType, "highland", null, null)
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }
}
