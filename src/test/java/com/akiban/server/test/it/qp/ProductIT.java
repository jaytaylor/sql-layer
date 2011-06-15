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
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.physicaloperator.API.*;
import static org.junit.Assert.assertTrue;

public class ProductIT extends PhysicalOperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] db = new NewRow[]{
            // Why 20: Because RowList initial capacity is 10. This tests RowList growth.
            createNewRow(customer, 1L, "northbridge"), // 20 orders, two addresses
            createNewRow(order, 100L, 1L, "ori"),
            createNewRow(order, 101L, 1L, "ori"),
            createNewRow(order, 102L, 1L, "ori"),
            createNewRow(order, 103L, 1L, "ori"),
            createNewRow(order, 104L, 1L, "ori"),
            createNewRow(order, 105L, 1L, "ori"),
            createNewRow(order, 106L, 1L, "ori"),
            createNewRow(order, 107L, 1L, "ori"),
            createNewRow(order, 108L, 1L, "ori"),
            createNewRow(order, 109L, 1L, "ori"),
            createNewRow(order, 110L, 1L, "ori"),
            createNewRow(order, 111L, 1L, "ori"),
            createNewRow(order, 112L, 1L, "ori"),
            createNewRow(order, 113L, 1L, "ori"),
            createNewRow(order, 114L, 1L, "ori"),
            createNewRow(order, 115L, 1L, "ori"),
            createNewRow(order, 116L, 1L, "ori"),
            createNewRow(order, 117L, 1L, "ori"),
            createNewRow(order, 118L, 1L, "ori"),
            createNewRow(order, 119L, 1L, "ori"),
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
            createNewRow(customer, 6L, "highland"), // no orders or addresses
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

    @Test
    public void testExtractRoot()
    {
        PhysicalOperator flattenCO =
            flatten_HKeyOrdered(groupScan_Default(coi), customerRowType, orderRowType, LEFT_JOIN | KEEP_PARENT);
        PhysicalOperator flattenCA =
            flatten_HKeyOrdered(flattenCO, customerRowType, addressRowType, LEFT_JOIN);
        PhysicalOperator plan = product_ByRun(flattenCA, flattenCO.rowType(), flattenCA.rowType());
        RowType coaRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 102L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 102L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 103L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 103L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 104L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 104L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 105L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 105L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 106L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 106L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 107L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 107L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 108L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 108L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 109L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 109L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 110L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 110L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 111L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 111L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 112L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 112L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 113L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 113L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 114L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 114L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 115L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 115L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 116L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 116L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 117L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 117L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 118L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 118L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 119L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 119L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
        };
        compareRows(expected, cursor);
    }
}
