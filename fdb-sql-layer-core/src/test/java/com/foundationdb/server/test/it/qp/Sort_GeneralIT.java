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

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class Sort_GeneralIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        Row[] dbRows = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(customer, 4L, "highland"),
            row(customer, 5L, "matrix"),
            row(order, 11L, 1L, "ori"),
            row(order, 12L, 1L, "david"),
            row(order, 21L, 2L, "david"),
            row(order, 22L, 2L, "jack"),
            row(order, 31L, 3L, "david"),
            row(order, 51L, 5L, "yuval"),
            row(item, 111L, 11L),
            row(item, 112L, 11L),
            row(item, 121L, 12L),
            row(item, 122L, 12L),
            row(item, 211L, 21L),
            row(item, 212L, 21L),
            row(item, 221L, 22L),
            row(item, 222L, 22L),
        };
        use(dbRows);
    }

    @Test
    public void testCustomerName()
    {
        Operator plan =
            sort_General(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType,
                ordering(field(customerRowType, 1), true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 1L, "northbridge"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesmanCid()
    {
        Operator plan =
            sort_General(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true, field(orderRowType, 1), false),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
            row(orderRowType, 11L, 1L, "ori"),
            row(orderRowType, 51L, 5L, "yuval"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesman()
    {
        Operator plan =
            sort_General(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                ordering(field(orderRowType, 2), true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            // Order among equals in group.
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
            row(orderRowType, 11L, 1L, "ori"),
            row(orderRowType, 51L, 5L, "yuval"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAAA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, true, iidField, true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAAD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, true, iidField, false),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testADA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, false, iidField, true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testADD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, true, oidField, false, iidField, false),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDAA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, true, iidField, true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDAD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, true, iidField, false),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDDA()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, false, iidField, true),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testDDD()
    {
        Operator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        ExpressionGenerator cidField = field(oiType, 1);
        ExpressionGenerator oidField = field(oiType, 0);
        ExpressionGenerator iidField = field(oiType, 3);
        Operator plan =
            sort_General(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                ordering(cidField, false, oidField, false, iidField, false),
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oiType, 22L, 2L, "jack", 222L, 22L),
            row(oiType, 22L, 2L, "jack", 221L, 22L),
            row(oiType, 21L, 2L, "david", 212L, 21L),
            row(oiType, 21L, 2L, "david", 211L, 21L),
            row(oiType, 12L, 1L, "david", 122L, 12L),
            row(oiType, 12L, 1L, "david", 121L, 12L),
            row(oiType, 11L, 1L, "ori", 112L, 11L),
            row(oiType, 11L, 1L, "ori", 111L, 11L),
        };
        compareRows(expected, cursor);
    }

    // Inspired by bug 871310
    @Test
    public void testReusedSort()
    {
        ExpressionGenerator iidField = field(itemRowType, 0);
        ExpressionGenerator oidField = field(itemRowType, 1);
        Operator plan =
            sort_General(
                sort_General(
                    filter_Default(
                        groupScan_Default(coi),
                        Collections.singleton(itemRowType)),
                    itemRowType,
                    ordering(iidField, true, oidField, true),
                    SortOption.PRESERVE_DUPLICATES),
                itemRowType,
                ordering(oidField, false, iidField, false),
                SortOption.PRESERVE_DUPLICATES);
        Row[] expected = new Row[]{
            row(itemRowType, 222L, 22L),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 212L, 21L),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 122L, 12L),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 112L, 11L),
            row(itemRowType, 111L, 11L),
        };
        for (int i = 0; i < 10; i++) {
            compareRows(expected, cursor(plan, queryContext, queryBindings));
        }
    }
    
    @Test
    public void testPreserveDuplicates()
    {
        Operator project = 
            project_DefaultTest(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Arrays.asList(field(orderRowType, 1)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_General(
                project,
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.PRESERVE_DUPLICATES);

        Row[] expected = new Row[]{
            row(projectType, 1L),
            row(projectType, 1L),
            row(projectType, 2L),
            row(projectType, 2L),
            row(projectType, 3L),
            row(projectType, 5L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testSuppressDuplicates()
    {
        Operator project =
            project_DefaultTest(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Arrays.asList(field(orderRowType, 1)));
        RowType projectType = project.rowType();
        Operator plan =
            sort_General(
                project,
                projectType,
                ordering(field(projectType, 0), true),
                SortOption.SUPPRESS_DUPLICATES);

        Row[] expected = new Row[]{
            row(projectType, 1L),
            row(projectType, 2L),
            row(projectType, 3L),
            row(projectType, 5L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            sort_General(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType,
                ordering(field(customerRowType, 1), true),
                SortOption.PRESERVE_DUPLICATES);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 4L, "highland"),
                    row(customerRowType, 5L, "matrix"),
                    row(customerRowType, 1L, "northbridge"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }


    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            ExpressionGenerator expression = (ExpressionGenerator) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }
}
