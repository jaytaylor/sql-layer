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

import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.IncompatibleRowException;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.physicaloperator.API.*;
import static com.akiban.qp.physicaloperator.API.FlattenOption.*;
import static com.akiban.qp.physicaloperator.API.JoinType.*;

public class FlattenIT extends PhysicalOperatorITBase
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
            createNewRow(item, 312L, 31L)};
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

    // TODO: Test parent/child relationship between parentType, childType:
    // TODO: - Unrelated
    // TODO: - parent = child
    // TODO: - parent is parent of child (OK)
    // TODO: - parent is ancestor of child
    // TODO: - child is parent of parent

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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    customerRowType,
                                                    orderRowType,
                                                    INNER_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    orderRowType,
                                                    itemRowType,
                                                    INNER_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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

    @Test(expected = IllegalArgumentException.class)
    public void shorteningWithoutLeftJoin() {
        flatten_HKeyOrdered(groupScan_Default(coi),
                orderRowType,
                itemRowType,
                INNER_JOIN, LEFT_JOIN_SHORTENS_HKEY);
    }

    @Test
    public void testLeftJoinCO()
    {
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    customerRowType,
                                                    orderRowType,
                                                    LEFT_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
    public void testLeftJoinCO_WithLeftShortenedHKey()
    {
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                customerRowType,
                orderRowType,
                LEFT_JOIN, LEFT_JOIN_SHORTENS_HKEY);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
                row(cKey(4L), coRowType, 4L, "highland", null, null, null),
                row(coRowType, 5L, "matrix", 51L, 5L, "yuval")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLeftJoinCOI_WithLeftShortenedHKey()
    {
        PhysicalOperator coPlan = flatten_HKeyOrdered(
                groupScan_Default(coi),
                customerRowType,
                orderRowType,
                LEFT_JOIN, LEFT_JOIN_SHORTENS_HKEY
        );
        RowType coRowType = coPlan.rowType();
        PhysicalOperator plan = flatten_HKeyOrdered(coPlan,
                coRowType,
                itemRowType,
                LEFT_JOIN, LEFT_JOIN_SHORTENS_HKEY
        );
        RowType coiRowType = plan.rowType();

        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
                row(iKey(1L, 11L, 111L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 111L, 11L),
                row(iKey(1L, 11L, 112L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 112L, 11L),
                row(iKey(1L, 12L, 121L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 121L, 12L),
                row(iKey(1L, 12L, 122L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 122L, 12L),

                row(iKey(2L, 21L, 211L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 211L, 21L),
                row(iKey(2L, 21L, 212L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 212L, 21L),
                row(iKey(2L, 22L, 221L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 221L, 22L),
                row(iKey(2L, 22L, 222L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 222L, 22L),

                row(cKey(4L), coiRowType, 4L, "highland", null, null, null, null, null),

                row(oKey(5L, 51L), coiRowType, 5L, "matrix", 51L, 5L, "yuval", null, null),
        };
        compareRows(expected, cursor);
    }

    @Test(expected = IncompatibleRowException.class)
    public void testLeftJoinCOI_WithPartiallyLeftShortenedHKey()
    {
        PhysicalOperator coPlan = flatten_HKeyOrdered(
                groupScan_Default(coi),
                customerRowType,
                orderRowType,
                LEFT_JOIN, LEFT_JOIN_SHORTENS_HKEY
        );
        RowType coRowType = coPlan.rowType();
        PhysicalOperator plan = flatten_HKeyOrdered(coPlan,
                coRowType,
                itemRowType,
                LEFT_JOIN
        );
        RowType coiRowType = plan.rowType();

        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
                row(iKey(1L, 11L, 111L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 111L, 11L),
                row(iKey(1L, 11L, 112L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 112L, 11L),
                row(iKey(1L, 12L, 121L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 121L, 12L),
                row(iKey(1L, 12L, 122L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 122L, 12L),

                row(iKey(2L, 21L, 211L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 211L, 21L),
                row(iKey(2L, 21L, 212L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 212L, 21L),
                row(iKey(2L, 22L, 221L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 221L, 22L),
                row(iKey(2L, 22L, 222L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 222L, 22L),

                row(oKey(4L, null), coiRowType, 4L, "highland", null, null, null, null, null),

                row(oKey(5L, 51L), coiRowType, 5L, "matrix", 51L, 5L, "yuval", null, null),
        };
        compareRows(expected, cursor);
    }

    @Test(expected = IncompatibleRowException.class)
    public void testLeftJoinCOI_WithFullKey()
    {
        PhysicalOperator coPlan = flatten_HKeyOrdered(
                groupScan_Default(coi),
                customerRowType,
                orderRowType,
                LEFT_JOIN, LEFT_JOIN_SHORTENS_HKEY
        );
        RowType coRowType = coPlan.rowType();
        PhysicalOperator plan = flatten_HKeyOrdered(coPlan,
                coRowType,
                itemRowType,
                LEFT_JOIN
        );
        RowType coiRowType = plan.rowType();

        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
                row(iKey(1L, 11L, 111L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 111L, 11L),
                row(iKey(1L, 11L, 112L), coiRowType, 1L, "northbridge", 11L, 1L, "ori", 112L, 11L),
                row(iKey(1L, 12L, 121L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 121L, 12L),
                row(iKey(1L, 12L, 122L), coiRowType, 1L, "northbridge", 12L, 1L, "david", 122L, 12L),

                row(iKey(2L, 21L, 211L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 211L, 21L),
                row(iKey(2L, 21L, 212L), coiRowType, 2L, "foundation", 21L, 2L, "tom", 212L, 21L),
                row(iKey(2L, 22L, 221L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 221L, 22L),
                row(iKey(2L, 22L, 222L), coiRowType, 2L, "foundation", 22L, 2L, "jack", 222L, 22L),

                row(iKey(4L, null, null), coiRowType, 4L, "highland", null, null, null, null, null),

                row(iKey(5L, 51L, null), coiRowType, 5L, "matrix", 51L, 5L, "yuval", null, null),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testLeftJoinOI()
    {
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    orderRowType,
                                                    itemRowType,
                                                    LEFT_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    customerRowType,
                                                    orderRowType,
                                                    RIGHT_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    orderRowType,
                                                    itemRowType,
                                                    RIGHT_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    customerRowType,
                                                    orderRowType,
                                                    FULL_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    orderRowType,
                                                    itemRowType,
                                                    FULL_JOIN);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    customerRowType,
                                                    orderRowType,
                                                    FULL_JOIN, KEEP_PARENT, KEEP_CHILD);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = flatten_HKeyOrdered(groupScan_Default(coi),
                                                    orderRowType,
                                                    itemRowType,
                                                    FULL_JOIN, KEEP_PARENT, KEEP_CHILD);
        RowType oiRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
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

    private String cKey(Long cid) {
        return String.format("{%d,%s}", customer, hKeyValue(cid));
    }

    private String oKey(Long cid, Long oid) {
        return String.format("{%d,%s,%d,%s}",customer, hKeyValue(cid), order, hKeyValue(oid));
    }

    private String iKey(Long cid, Long oid, Long iid) {
        return String.format("{%d,%s,%d,%s,%d,%s}",customer, hKeyValue(cid), order, hKeyValue(oid), item, hKeyValue(iid));
    }

    private String hKeyValue(Long x)
    {
        return x == null ? "null" : String.format("(long)%d", x);
    }
}
