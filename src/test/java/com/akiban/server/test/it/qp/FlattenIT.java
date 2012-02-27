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

import java.util.Arrays;
import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.FlattenOption.*;
import static com.akiban.qp.operator.API.JoinType.*;
import static org.junit.Assert.assertTrue;

public class FlattenIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(customerRowType, 5L, "matrix")
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval")
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
        Cursor cursor = cursor(plan, queryContext);
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
            row(oiRowType, 51L, 5L, "yuval", null, null)
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval")
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(customerRowType, 5L, "matrix")
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval")
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(oiRowType, 51L, 5L, "yuval", null, null)
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
            row(coRowType, 5L, "matrix", 51L, 5L, "yuval")
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
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
        Cursor cursor = cursor(flattenCO, queryContext);
        RowBase[] expected = new RowBase[]{
            row(flattenCO.rowType(), 1L, "northbridge", 11L, 1L, "ori"),
            row(flattenCO.rowType(), 1L, "northbridge", 12L, 1L, "david"),
            row(flattenCO.rowType(), 2L, "foundation", 21L, 2L, "tom"),
            row(flattenCO.rowType(), 2L, "foundation", 22L, 2L, "jack"),
            row(flattenCO.rowType(), 4L, "highland", null, null, null),
            row(flattenCA.rowType(), 4L, "highland", 41L, 4L, "560 Harrison"),
            row(flattenCO.rowType(), 5L, "matrix", 51L, 5L, "yuval"),
        };
        compareRows(expected, cursor);
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

    private String hKeyValue(Long x)
    {
        return x == null ? "null" : String.format("(long)%d", x);
    }
}
