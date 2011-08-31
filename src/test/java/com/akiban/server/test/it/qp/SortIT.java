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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.expression.API.field;
import static com.akiban.qp.physicaloperator.API.*;

public class SortIT extends PhysicalOperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] dbRows = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(customer, 5L, "matrix"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "david"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(order, 31L, 3L, "david"),
            createNewRow(order, 51L, 5L, "yuval"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L),
        };
        use(dbRows);
    }

    // Sort / InsertionLimited tests

    @Test
    public void testCustomerName()
    {
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                customerRowType,
                Collections.singletonList(field(1)),
                Collections.singletonList(Boolean.FALSE),
                2);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 2L, "foundation"),
            row(customerRowType, 4L, "highland")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesmanCid()
    {
        List<Expression> sortExpressions = new ArrayList<Expression>(2);
        List<Boolean> sortDescendings = new ArrayList<Boolean>(2);
        sortExpressions.add(field(2));
        sortDescendings.add(Boolean.FALSE);
        sortExpressions.add(field(1));
        sortDescendings.add(Boolean.TRUE);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                sortExpressions,
                sortDescendings,
                4);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesman()
    {
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Collections.singletonList(field(2)),
                Collections.singletonList(Boolean.FALSE),
                4);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            // Order among equals is group.
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 21L, 2L, "david"),
            row(orderRowType, 31L, 3L, "david"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderSalesman2()
    {
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                orderRowType,
                Collections.singletonList(field(2)),
                Collections.singletonList(Boolean.FALSE),
                2);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            // Kept earlier ones in group (fewer inserts).
            row(orderRowType, 12L, 1L, "david"),
            row(orderRowType, 21L, 2L, "david"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAAA()
    {
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(false, false, false),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(false, false, true),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(false, true, false),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(false, true, true),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(true, false, false),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(true, false, true),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(true, true, false),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator flattenOI = flatten_HKeyOrdered(
            groupScan_Default(coi),
            orderRowType,
            itemRowType,
            JoinType.INNER_JOIN);
        RowType oiType = flattenOI.rowType();
        // flattenOI columns: oid, cid, salesman, iid, oid
        Expression cidField = field(1);
        Expression oidField = field(0);
        Expression iidField = field(3);
        PhysicalOperator plan =
            sort_InsertionLimited(
                filter_Default(
                    flattenOI,
                    Collections.singleton(oiType)),
                oiType,
                Arrays.asList(cidField, oidField, iidField),
                Arrays.asList(true, true, true),
                8);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
}
