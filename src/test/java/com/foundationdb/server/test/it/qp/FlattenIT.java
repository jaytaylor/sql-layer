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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.FlattenOption.KEEP_CHILD;
import static com.foundationdb.qp.operator.API.FlattenOption.KEEP_PARENT;
import static com.foundationdb.qp.operator.API.JoinType.*;
import static org.junit.Assert.assertTrue;

public class FlattenIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        NewRow[] dbWithOrphans = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(customer, 5L, "matrix"), // customer 5 is for testing bug 792102
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(order, 31L, 3L, "peter"),
            createNewRow(order, 51L, 5L, "yuval"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L),
            // orphans
            createNewRow(item, 311L, 31L),
            createNewRow(item, 312L, 31L),
            // Bug 837706
            createNewRow(address, 41L, 4L, "560 Harrison"),
            // Bug 1018206
            createNewRow(customer, 6L, "nea"),
            createNewRow(order, 61L, 6L, "mike"),
            createNewRow(order, 62L, 6L, "padraig"),
            createNewRow(item, 621L, 62L),
        };
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullParent()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            null,
                            orderRowType,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullChild()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            customerRowType,
                            null,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParentChild_Unrelated()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            addressRowType,
                            orderRowType,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParentChild_SameType()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            orderRowType,
                            orderRowType,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test
    public void testParentChild_BaseTables()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            customerRowType,
                            orderRowType,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParentChild_UpsideDown()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            orderRowType,
                            customerRowType,
                            INNER_JOIN,
                            EnumSet.noneOf(FlattenOption.class));
    }

    @Test
    public void testParentChild_FlattenedParent()
    {
        Operator co =
            flatten_HKeyOrdered(groupScan_Default(coi),
                               customerRowType,
                               orderRowType,
                               INNER_JOIN,
                               EnumSet.noneOf(FlattenOption.class));
        flatten_HKeyOrdered(groupScan_Default(coi),
                           co.rowType(),
                           itemRowType,
                           INNER_JOIN,
                           EnumSet.noneOf(FlattenOption.class));
    }

    @Test
    public void testParentChild_FlattenedChild()
    {
        Operator oi =
            flatten_HKeyOrdered(groupScan_Default(coi),
                               orderRowType,
                               itemRowType,
                               INNER_JOIN,
                               EnumSet.noneOf(FlattenOption.class));
        flatten_HKeyOrdered(groupScan_Default(coi),
                           customerRowType,
                           oi.rowType(),
                           INNER_JOIN,
                           EnumSet.noneOf(FlattenOption.class));
    }

    @Test
    public void testParentChild_GrandParent()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                           customerRowType,
                           itemRowType,
                           INNER_JOIN,
                           EnumSet.noneOf(FlattenOption.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullJoinType()
    {
        flatten_HKeyOrdered(groupScan_Default(coi),
                            customerRowType,
                            orderRowType,
                            null,
                            EnumSet.noneOf(FlattenOption.class));
    }

    // Tests of join behavior

    @Test
    public void testInnerJoinCO()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                customerRowType,
                orderRowType,
                INNER_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(coRowType, 1L, "northbridge", 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(coRowType, 1L, "northbridge", 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(coRowType, 2L, "foundation", 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(coRowType, 2L, "foundation", 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval"),
            row(coRowType, 6L, "nea", 61L, 6L, "mike"),
            row(coRowType, 6L, "nea", 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testInnerJoinOI()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                INNER_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(oiRowType, 11L, 1L, "ori", 111L, 11L),
            row(oiRowType, 11L, 1L, "ori", 112L, 11L),
            row(oiRowType, 12L, 1L, "david", 121L, 12L),
            row(oiRowType, 12L, 1L, "david", 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(oiRowType, 21L, 2L, "tom", 211L, 21L),
            row(oiRowType, 21L, 2L, "tom", 212L, 21L),
            row(oiRowType, 22L, 2L, "jack", 221L, 22L),
            row(oiRowType, 22L, 2L, "jack", 222L, 22L),
            row(oiRowType, 31L, 3L, "peter", 311L, 31L),
            row(oiRowType, 31L, 3L, "peter", 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 6L, "nea"),
            row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLeftJoinCO()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                customerRowType,
                orderRowType,
                LEFT_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(oKey(1L, 11L), coRowType, 1L, "northbridge", 11L, 1L, "ori"),
            row(iKey(1L, 11L, 111L), itemRowType, 111L, 11L),
            row(iKey(1L, 11L, 112L), itemRowType, 112L, 11L),
            row(coRowType, 1L, "northbridge", 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(coRowType, 2L, "foundation", 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(coRowType, 2L, "foundation", 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(oKey(4L, null), coRowType, 4L, "highland", null, null, null),
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval"),
            row(coRowType, 6L, "nea", 61L, 6L, "mike"),
            row(coRowType, 6L, "nea", 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLeftJoinOI()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                LEFT_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        TestRow[] expected = new TestRow[]{
            row(customerRowType, 1L, "northbridge"),
            row(oiRowType, 11L, 1L, "ori", 111L, 11L),
            row(oiRowType, 11L, 1L, "ori", 112L, 11L),
            row(oiRowType, 12L, 1L, "david", 121L, 12L),
            row(oiRowType, 12L, 1L, "david", 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(oiRowType, 21L, 2L, "tom", 211L, 21L),
            row(oiRowType, 21L, 2L, "tom", 212L, 21L),
            row(oiRowType, 22L, 2L, "jack", 221L, 22L),
            row(oiRowType, 22L, 2L, "jack", 222L, 22L),
            row(oiRowType, 31L, 3L, "peter", 311L, 31L),
            row(oiRowType, 31L, 3L, "peter", 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(oiRowType, 51L, 5L, "yuval", null, null),
            row(customerRowType, 6L, "nea"),
            row(oiRowType, 61L, 6L, "mike", null, null),
            row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testRightJoinCO()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                customerRowType,
                orderRowType,
                RIGHT_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(coRowType, 1L, "northbridge", 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(coRowType, 1L, "northbridge", 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(coRowType, 2L, "foundation", 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(coRowType, 2L, "foundation", 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(coRowType, null, null, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval"),
            row(coRowType, 6L, "nea", 61L, 6L, "mike"),
            row(coRowType, 6L, "nea", 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testRightJoinOI()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                RIGHT_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(oiRowType, 11L, 1L, "ori", 111L, 11L),
            row(oiRowType, 11L, 1L, "ori", 112L, 11L),
            row(oiRowType, 12L, 1L, "david", 121L, 12L),
            row(oiRowType, 12L, 1L, "david", 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(oiRowType, 21L, 2L, "tom", 211L, 21L),
            row(oiRowType, 21L, 2L, "tom", 212L, 21L),
            row(oiRowType, 22L, 2L, "jack", 221L, 22L),
            row(oiRowType, 22L, 2L, "jack", 222L, 22L),
            row(oiRowType, 31L, 3L, "peter", 311L, 31L),
            row(oiRowType, 31L, 3L, "peter", 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(customerRowType, 6L, "nea"),
            row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFullJoinCO()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                customerRowType,
                orderRowType,
                FULL_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(coRowType, 1L, "northbridge", 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(coRowType, 1L, "northbridge", 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(coRowType, 2L, "foundation", 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(coRowType, 2L, "foundation", 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(coRowType, null, null, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(coRowType, 4L, "highland", null, null, null),
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval"),
            row(coRowType, 6L, "nea", 61L, 6L, "mike"),
            row(coRowType, 6L, "nea", 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFullJoinOI()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                FULL_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(oiRowType, 11L, 1L, "ori", 111L, 11L),
            row(oiRowType, 11L, 1L, "ori", 112L, 11L),
            row(oiRowType, 12L, 1L, "david", 121L, 12L),
            row(oiRowType, 12L, 1L, "david", 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(oiRowType, 21L, 2L, "tom", 211L, 21L),
            row(oiRowType, 21L, 2L, "tom", 212L, 21L),
            row(oiRowType, 22L, 2L, "jack", 221L, 22L),
            row(oiRowType, 22L, 2L, "jack", 222L, 22L),
            row(oiRowType, 31L, 3L, "peter", 311L, 31L),
            row(oiRowType, 31L, 3L, "peter", 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(oiRowType, 51L, 5L, "yuval", null, null),
            row(customerRowType, 6L, "nea"),
            row(oiRowType, 61L, 6L, "mike", null, null),
            row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    // Tests of keep input row behavior
    // TODO: Ordering between flattened row and input child row is not well defined. They have matching hkeys,
    // TODO: e.g., for order and co.

    @Test
    public void testFullJoinCOKeepParentAndChild()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                customerRowType,
                orderRowType,
                FULL_JOIN, KEEP_PARENT, KEEP_CHILD);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(coRowType, 1L, "northbridge", 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(coRowType, 1L, "northbridge", 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(coRowType, 2L, "foundation", 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(coRowType, 2L, "foundation", 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(coRowType, null, null, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(coRowType, 4L, "highland", null, null, null),
            row(customerRowType, 5L, "matrix"),
            row(orderRowType, 51L, 5L, "yuval"),
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval"),
            row(customerRowType, 6L, "nea"),
            row(orderRowType, 61L, 6L, "mike"),
            row(coRowType, 6L, "nea", 61L, 6L, "mike"),
            row(orderRowType, 62L, 6L, "padraig"),
            row(coRowType, 6L, "nea", 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testFullJoinOIKeepParentAndChild()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                FULL_JOIN, KEEP_PARENT, KEEP_CHILD);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(oiRowType, 11L, 1L, "ori", 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(oiRowType, 11L, 1L, "ori", 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(oiRowType, 12L, 1L, "david", 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(oiRowType, 12L, 1L, "david", 122L, 12L),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(oiRowType, 21L, 2L, "tom", 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(oiRowType, 21L, 2L, "tom", 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(oiRowType, 22L, 2L, "jack", 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(oiRowType, 22L, 2L, "jack", 222L, 22L),
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L),
            row(oiRowType, 31L, 3L, "peter", 311L, 31L),
            row(itemRowType, 312L, 31L),
            row(oiRowType, 31L, 3L, "peter", 312L, 31L),
            row(customerRowType, 4L, "highland"),
            row(customerRowType, 5L, "matrix"),
            row(orderRowType, 51L, 5L, "yuval"),
            row(oiRowType, 51L, 5L, "yuval", null, null),
            row(customerRowType, 6L, "nea"),
            row(orderRowType, 61L, 6L, "mike"),
            row(oiRowType, 61L, 6L, "mike", null, null),
            row(orderRowType, 62L, 6L, "padraig"),
            row(itemRowType, 621L, 62L),
            row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testBug837706()
    {
        // inner join CA followed by left join CO, for a missing order. The (customer, null) row for the missing order
        // appears after the (customer, address) row, which is wrong.
        assertTrue(ordinal(customerRowType) < ordinal(orderRowType));
        assertTrue(ordinal(orderRowType) < ordinal(addressRowType));
        Operator flattenCA =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, addressRowType)),
                customerRowType,
                addressRowType,
                INNER_JOIN,
                KEEP_PARENT);
        Operator flattenCO =
            flatten_HKeyOrdered(
                flattenCA,
                customerRowType,
                orderRowType,
                LEFT_JOIN);
        Cursor cursor = cursor(flattenCO, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(flattenCO.rowType(), 1L, "northbridge", 11L, 1L, "ori"),
            row(flattenCO.rowType(), 1L, "northbridge", 12L, 1L, "david"),
            row(flattenCO.rowType(), 2L, "foundation", 21L, 2L, "tom"),
            row(flattenCO.rowType(), 2L, "foundation", 22L, 2L, "jack"),
            row(flattenCO.rowType(), 4L, "highland", null, null, null),
            row(flattenCA.rowType(), 4L, "highland", 41L, 4L, "560 Harrison"),
            row(flattenCO.rowType(), 5L, "matrix", 51L, 5L, "yuval"),
            row(flattenCO.rowType(), 6L, "nea", 61L, 6L, "mike"),
            row(flattenCO.rowType(), 6L, "nea", 62L, 6L, "padraig"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testBug1018206()
    {
        IndexBound cnameBound = new IndexBound(row(customerNameItemOidIndexRowType, "nea"), new SetColumnSelector(0));
        IndexKeyRange cnameRange = IndexKeyRange.bounded(customerNameItemOidIndexRowType, cnameBound, true, cnameBound, true);
        IndexScanSelector indexScanSelector = IndexScanSelector.leftJoinAfter(customerNameItemOidIndexRowType.index(),
                                                                              customerRowType.table());
        Operator flatten =
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(customerNameItemOidIndexRowType, cnameRange, ordering(), indexScanSelector),
                    coi,
                    customerNameItemOidIndexRowType,
                    Arrays.asList(customerRowType, itemRowType),
                    InputPreservationOption.DISCARD_INPUT),
                customerRowType,
                itemRowType,
                LEFT_JOIN);
        Row[] expected = new Row[] {
            row(flatten.rowType(), 6L, "nea", null, null),
            row(flatten.rowType(), 6L, "nea", 621L, 62L),
        };
        compareRows(expected, cursor(flatten, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, orderRowType, itemRowType)),
                orderRowType,
                itemRowType,
                FULL_JOIN);
        final RowType oiRowType = plan.rowType();
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                row(customerRowType, 1L, "northbridge"),
                    row(oiRowType, 11L, 1L, "ori", 111L, 11L),
                    row(oiRowType, 11L, 1L, "ori", 112L, 11L),
                    row(oiRowType, 12L, 1L, "david", 121L, 12L),
                    row(oiRowType, 12L, 1L, "david", 122L, 12L),
                    row(customerRowType, 2L, "foundation"),
                    row(oiRowType, 21L, 2L, "tom", 211L, 21L),
                    row(oiRowType, 21L, 2L, "tom", 212L, 21L),
                    row(oiRowType, 22L, 2L, "jack", 221L, 22L),
                    row(oiRowType, 22L, 2L, "jack", 222L, 22L),
                    row(oiRowType, 31L, 3L, "peter", 311L, 31L),
                    row(oiRowType, 31L, 3L, "peter", 312L, 31L),
                    row(customerRowType, 4L, "highland"),
                    row(customerRowType, 5L, "matrix"),
                    row(oiRowType, 51L, 5L, "yuval", null, null),
                    row(customerRowType, 6L, "nea"),
                    row(oiRowType, 61L, 6L, "mike", null, null),
                    row(oiRowType, 62L, 6L, "padraig", 621L, 62L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private String cKey(Long cid)
    {
        return String.format("{%d,%s}", customerOrdinal, hKeyValue(cid));
    }

    private String oKey(Long cid, Long oid)
    {
        return String.format("{%d,%s,%d,%s}", customerOrdinal, hKeyValue(cid), orderOrdinal, hKeyValue(oid));
    }

    private String iKey(Long cid, Long oid, Long iid)
    {
        return String.format("{%d,%s,%d,%s,%d,%s}", customerOrdinal, hKeyValue(cid), orderOrdinal, hKeyValue(oid), itemOrdinal, hKeyValue(iid));
    }

    private IndexRowType giRowType;
}
