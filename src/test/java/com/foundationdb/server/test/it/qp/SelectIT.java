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
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

import org.junit.Test;

import static com.foundationdb.server.test.ExpressionGenerators.compare;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static com.foundationdb.server.test.ExpressionGenerators.literal;
import static com.foundationdb.qp.operator.API.*;

public class SelectIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
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
        select_HKeyOrdered(groupScan_Default(coi), customerRowType, (TPreparedExpression)null);
    }

    // Runtime tests

    @Test
    public void testSelectCustomer()
    {
        Operator plan =
            select_HKeyOrdered(
                groupScan_Default(coi), customerRowType, customerNameEQ("northbridge"));
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
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
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
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
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
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

    @Test
    public void testCursor()
    {
        Operator plan =
            select_HKeyOrdered(
                groupScan_Default(coi), itemRowType, itemOidEQ(12L));
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
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
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // For use by this class

    private ExpressionGenerator customerNameEQ(String name)
    {
        return compare(field(customerRowType, 1), Comparison.EQ, literal(name), castResolver());
    }

    private ExpressionGenerator orderSalesmanEQ(String name)
    {
        return compare(field(orderRowType, 2), Comparison.EQ, literal(name), castResolver());
    }

    private ExpressionGenerator itemOidEQ(long oid)
    {
        return compare(field(itemRowType, 1), Comparison.EQ, literal(oid), castResolver());
    }
}
