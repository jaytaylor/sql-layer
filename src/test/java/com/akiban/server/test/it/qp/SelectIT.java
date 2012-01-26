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
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.server.expression.std.Expressions.compare;
import static com.akiban.server.expression.std.Expressions.field;
import static com.akiban.server.expression.std.Expressions.literal;
import static com.akiban.qp.operator.API.*;

public class SelectIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] dbWithOrphans = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(customer, 4L, "highland"),
            createNewRow(address, 1001L, 1L, "111 1111 st"),
            createNewRow(address, 1002L, 1L, null),
            createNewRow(address, 2001L, 2L, "222 1111 st"),
            createNewRow(address, 2002L, 2L, null),
            createNewRow(address, 4001L, 4L, "444 1111 st"),
            createNewRow(address, 4002L, 4L, null),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, null),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, null),
            createNewRow(order, 31L, 3L, "peter"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L),
            // orphans
            createNewRow(address, 5001L, 5L, "555 1111 st"),
            createNewRow(item, 311L, 31L),
            createNewRow(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullPredicateRowType()
    {
        select_HKeyOrdered(groupScan_Default(coi), null, customerNameEQ("northbridge"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPredicate()
    {
        select_HKeyOrdered(groupScan_Default(coi), customerRowType, null);
    }

    // Runtime tests

    @Test
    public void testSelectCustomer()
    {
        Operator plan =
            select_HKeyOrdered(
                groupScan_Default(coi), customerRowType, customerNameEQ("northbridge"));
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, null),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, null),
        };
        compareRows(expected, cursor);
    }

    // Includes test of null column comparison
    @Test
    public void testSelectOrder()
    {
        Operator plan =
            select_HKeyOrdered(
                groupScan_Default(coi), orderRowType, orderSalesmanEQ("tom"));
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, null),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, null),
            row(customerRowType, 4L, "highland"),
            row(addressRowType, 4001L, 4L, "444 1111 st"),
            row(addressRowType, 4002L, 4L, null),
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testSelectItem()
    {
        Operator plan =
            select_HKeyOrdered(
                groupScan_Default(coi), itemRowType, itemOidEQ(12L));
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(orderRowType, 12L, 1L, null),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, null),
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(orderRowType, 22L, 2L, null),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, null),
            row(orderRowType, 31L, 3L, "peter"),
            row(customerRowType, 4L, "highland"),
            row(addressRowType, 4001L, 4L, "444 1111 st"),
            row(addressRowType, 4002L, 4L, null),
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    // For use by this class

    private Expression customerNameEQ(String name)
    {
        return compare(field(customerRowType, 1), Comparison.EQ, literal(name));
    }

    private Expression orderSalesmanEQ(String name)
    {
        return compare(field(orderRowType, 2), Comparison.EQ, literal(name));
    }

    private Expression itemOidEQ(long oid)
    {
        return compare(field(itemRowType, 1), Comparison.EQ, literal(oid));
    }
}
