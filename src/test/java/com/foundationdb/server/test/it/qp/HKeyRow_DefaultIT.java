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
import com.foundationdb.qp.rowtype.HKeyRowType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.literal;

public class HKeyRow_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        customerHKeyRowType = schema.newHKeyRowType(table(customer).hKey());
        orderHKeyRowType = schema.newHKeyRowType(table(order).hKey());
        itemHKeyRowType = schema.newHKeyRowType(table(item).hKey());
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testNullRowType() {
        hKeyRow_DefaultTest(null, Arrays.asList(literal(1L)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotHKeyRowType() {
        hKeyRow_DefaultTest(customerRowType, Arrays.asList(literal(1L)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullExpressions() {
        hKeyRow_DefaultTest(customerHKeyRowType, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongExpressionsLength() {
        hKeyRow_DefaultTest(customerHKeyRowType, Arrays.asList(literal(1L),literal(2L)));
    }

    // Test operator execution

    @Test
    public void testItemRow() {
        Operator plan =
            groupLookup_Default(
                hKeyRow_DefaultTest(itemHKeyRowType, Arrays.asList(literal(1L), literal(12L), literal(121L))),
                coi, itemHKeyRowType, Arrays.asList(itemRowType),
                InputPreservationOption.DISCARD_INPUT, 0);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 121L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderBranch() {
        Operator plan =
            branchLookup_Default(
                hKeyRow_DefaultTest(orderHKeyRowType, Arrays.asList(literal(2L), literal(22L))),
                coi, orderHKeyRowType, orderRowType,
                InputPreservationOption.DISCARD_INPUT);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor() {
        Operator plan =
            groupLookup_Default(
                hKeyRow_DefaultTest(customerHKeyRowType, Arrays.asList(literal(1L))),
                coi, customerHKeyRowType, Arrays.asList(customerRowType),
                InputPreservationOption.DISCARD_INPUT, 0);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 1L, "xyz"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    protected HKeyRowType customerHKeyRowType;
    protected HKeyRowType orderHKeyRowType;
    protected HKeyRowType itemHKeyRowType;
}
