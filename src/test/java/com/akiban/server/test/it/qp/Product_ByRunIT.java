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
import com.akiban.qp.operator.IncompatibleRowException;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.AisRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.akiban.qp.operator.API.*;
import static com.akiban.qp.operator.API.JoinType.*;
import static com.akiban.qp.operator.API.FlattenOption.*;
import static org.junit.Assert.assertTrue;

public class Product_ByRunIT extends OperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] db = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"), // two orders, two addresses
            createNewRow(order, 100L, 1L, "ori"),
            createNewRow(order, 101L, 1L, "ori"),
            createNewRow(address, 1000L, 1L, "111 1000 st"),
            createNewRow(address, 1001L, 1L, "111 1001 st"),
            createNewRow(customer, 2L, "foundation"), // two orders, one address
            createNewRow(order, 200L, 2L, "david"),
            createNewRow(order, 201L, 2L, "david"),
            createNewRow(address, 2000L, 2L, "222 2000 st"),
            createNewRow(customer, 3L, "matrix"), // one order, two addresses
            createNewRow(order, 300L, 3L, "tom"),
            createNewRow(address, 3000L, 3L, "333 3000 st"),
            createNewRow(address, 3001L, 3L, "333 3001 st"),
            createNewRow(customer, 4L, "atlas"), // two orders, no addresses
            createNewRow(order, 400L, 4L, "jack"),
            createNewRow(order, 401L, 4L, "jack"),
            createNewRow(customer, 5L, "highland"), // no orders, two addresses
            createNewRow(address, 5000L, 5L, "555 5000 st"),
            createNewRow(address, 5001L, 5L, "555 5001 st"),
            createNewRow(customer, 6L, "flybridge"), // no orders or addresses
            // Add a few items to test Product_ByRun rejecting unexpected input. All other tests remove these items.
            createNewRow(item, 1000L, 100L),
            createNewRow(item, 1001L, 100L),
            createNewRow(item, 1010L, 101L),
            createNewRow(item, 1011L, 101L),
            createNewRow(item, 2000L, 200L),
            createNewRow(item, 2001L, 200L),
            createNewRow(item, 2010L, 201L),
            createNewRow(item, 2011L, 201L),
            createNewRow(item, 3000L, 300L),
            createNewRow(item, 3001L, 300L),
            createNewRow(item, 4000L, 400L),
            createNewRow(item, 4001L, 400L),
            createNewRow(item, 4010L, 401L),
            createNewRow(item, 4011L, 401L),
        };
        use(db);
    }

    // Test assumption about ordinals

    @Test
    public void ordersBeforeAddresses()
    {
        assertTrue(ordinal(orderRowType) < ordinal(addressRowType));
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testLeftNull()
    {
        product_ByRun(groupScan_Default(coi), customerRowType, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightNull()
    {
        product_ByRun(groupScan_Default(coi), null, orderRowType);
    }

    // Test operator execution

    @Test(expected = IncompatibleRowException.class)
    public void testGroupScanInput()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                groupScan_Default(coi),
                customerRowType,
                orderRowType,
                INNER_JOIN,
                KEEP_PARENT);
        Operator flattenCA =
            flatten_HKeyOrdered(flattenCO,
                                customerRowType,
                                addressRowType,
                                INNER_JOIN);
        Operator plan = product_ByRun(flattenCA, flattenCO.rowType(), flattenCA.rowType());
        scan(cursor(plan, queryContext));
    }

    @Test(expected = IncompatibleRowException.class)
    public void testUnflattenedProductInput()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                branchLookup_Default(
                    indexScan_Default(customerNameIndexRowType, false),
                    coi,
                    customerNameIndexRowType,
                    customerRowType,
                    LookupOption.DISCARD_INPUT),
                customerRowType,
                orderRowType,
                INNER_JOIN,
                KEEP_PARENT);
        Operator flattenCA =
            flatten_HKeyOrdered(flattenCO,
                                customerRowType,
                                addressRowType,
                                INNER_JOIN);
        Operator plan = product_ByRun(flattenCA, flattenCO.rowType(), flattenCA.rowType());
        scan(cursor(plan, queryContext));
    }

    @Test
    public void testProductAfterIndexScanOfRoot()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                filter_Default(
                    branchLookup_Default(
                        indexScan_Default(customerNameIndexRowType, false),
                        coi,
                        customerNameIndexRowType,
                        customerRowType,
                        LookupOption.DISCARD_INPUT),
                    removeDescendentTypes(orderRowType)),
                customerRowType,
                orderRowType,
                INNER_JOIN,
                KEEP_PARENT);
        Operator flattenCA =
            flatten_HKeyOrdered(flattenCO,
                                customerRowType,
                                addressRowType,
                                INNER_JOIN);
        Operator plan = product_ByRun(flattenCA, flattenCO.rowType(), flattenCA.rowType());
        RowType coaRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProductAfterIndexScanOfNonRoot()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                filter_Default(
                    branchLookup_Default(
                        indexScan_Default(orderSalesmanIndexRowType, false),
                        coi,
                        orderSalesmanIndexRowType,
                        customerRowType,
                        LookupOption.DISCARD_INPUT),
                    removeDescendentTypes(orderRowType)),
                customerRowType,
                orderRowType,
                INNER_JOIN,
                KEEP_PARENT);
        Operator flattenCA =
            flatten_HKeyOrdered(flattenCO,
                                customerRowType,
                                addressRowType,
                                INNER_JOIN);
        Operator plan = product_ByRun(flattenCA, flattenCO.rowType(), flattenCA.rowType());
        RowType coaRowType = plan.rowType();
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
        };
        compareRows(expected, cursor);
    }

    // TODO: Test handling of rows whose type is not involved in product.

    private Set<AisRowType> removeDescendentTypes(AisRowType type)
    {
        Set<AisRowType> keepTypes = type.schema().userTableTypes();
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }
}
