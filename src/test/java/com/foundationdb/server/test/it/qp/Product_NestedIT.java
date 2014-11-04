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
import com.foundationdb.qp.rowtype.AisRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.Set;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.JoinType.INNER_JOIN;
import static com.foundationdb.qp.rowtype.RowTypeChecks.checkRowTypeFields;
import static org.junit.Assert.assertTrue;

public class Product_NestedIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema()
    {
        super.setupPostCreateSchema();
        Row[] db = new Row[]{
            row(customer, 1L, "northbridge"), // two orders, two addresses
            row(order, 100L, 1L, "ori"),
            row(order, 101L, 1L, "ori"),
            row(address, 1000L, 1L, "111 1000 st"),
            row(address, 1001L, 1L, "111 1001 st"),
            row(customer, 2L, "foundation"), // two orders, one address
            row(order, 200L, 2L, "david"),
            row(order, 201L, 2L, "david"),
            row(address, 2000L, 2L, "222 2000 st"),
            row(customer, 3L, "matrix"), // one order, two addresses
            row(order, 300L, 3L, "tom"),
            row(address, 3000L, 3L, "333 3000 st"),
            row(address, 3001L, 3L, "333 3001 st"),
            row(customer, 4L, "atlas"), // two orders, no addresses
            row(order, 400L, 4L, "jack"),
            row(order, 401L, 4L, "jack"),
            row(customer, 5L, "highland"), // no orders, two addresses
            row(address, 5000L, 5L, "555 5000 st"),
            row(address, 5001L, 5L, "555 5001 st"),
            row(customer, 6L, "flybridge"), // no orders or addresses
            // Add a few items to test Product_ByRun rejecting unexpected input. All other tests remove these items.
            row(item, 1000L, 100L),
            row(item, 1001L, 100L),
            row(item, 1010L, 101L),
            row(item, 1011L, 101L),
            row(item, 2000L, 200L),
            row(item, 2001L, 200L),
            row(item, 2010L, 201L),
            row(item, 2011L, 201L),
            row(item, 3000L, 300L),
            row(item, 3001L, 300L),
            row(item, 4000L, 400L),
            row(item, 4001L, 400L),
            row(item, 4010L, 401L),
            row(item, 4011L, 401L),
        };
        use(db);
    }

    protected int lookaheadQuantum() {
        return 1;
    }

    // Test assumption about ordinals

    @Test
    public void ordersBeforeAddresses()
    {
        assertTrue(ordinal(orderRowType) < ordinal(addressRowType));
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testInputNull()
    {
        product_Nested(null, customerRowType, null, customerRowType, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLeftTypeNull()
    {
        product_Nested(groupScan_Default(coi), null, null, customerRowType, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightTypeNull()
    {
        product_Nested(groupScan_Default(coi), customerRowType, null, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeInputBindingPosition()
    {
        product_Nested(groupScan_Default(coi), customerRowType, null, customerRowType, -1);
    }

    // Test operator execution

    // TODO: If inner input has rows of unexpected types, (not of innerType), should an IncompatibleRowException be thrown?

    @Test
    public void testProductAfterIndexScanOfRoot()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                filter_Default(
                    branchLookup_Default(
                        ancestorLookup_Default(
                            indexScan_Default(customerNameIndexRowType, false),
                            coi,
                            customerNameIndexRowType,
                            Collections.singleton(customerRowType),
                            InputPreservationOption.DISCARD_INPUT),
                        coi,
                        customerRowType,
                        orderRowType,
                        InputPreservationOption.KEEP_INPUT),
                    removeDescendentTypes(orderRowType)),
                customerRowType,
                orderRowType,
                INNER_JOIN);
        Operator flattenCA =
            flatten_HKeyOrdered(
                branchLookup_Nested(coi, flattenCO.rowType(), customerRowType, null, list(addressRowType), InputPreservationOption.KEEP_INPUT, 0, lookaheadQuantum()),
                customerRowType,
                addressRowType,
                INNER_JOIN);
        Operator product = product_Nested(flattenCA, flattenCO.rowType(), null, flattenCA.rowType(), 0);
        RowType coaRowType = product.rowType();
        checkRowTypeFields(null, coaRowType, 
                MNumeric.INT.instance(false),
                MString.VARCHAR.instance(20, true),
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(20, true),
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(100, true));
        
        Operator plan = map_NestedLoops(flattenCO, product, 0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProductAfterIndexScanOfNonRoot()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(orderSalesmanIndexRowType, false),
                    coi,
                    orderSalesmanIndexRowType,
                    Arrays.asList(orderRowType, customerRowType),
                    InputPreservationOption.DISCARD_INPUT),
                customerRowType,
                orderRowType,
                INNER_JOIN);
        Operator flattenCA =
            flatten_HKeyOrdered(
                branchLookup_Nested(coi, flattenCO.rowType(), customerRowType, null, list(addressRowType), InputPreservationOption.KEEP_INPUT, 0, lookaheadQuantum()),
                customerRowType,
                addressRowType,
                INNER_JOIN);
        Operator product = product_Nested(flattenCA, flattenCO.rowType(), null, flattenCA.rowType(), 0);
        RowType coaRowType = product.rowType();
        
        checkRowTypeFields(null, coaRowType, 
                MNumeric.INT.instance(false),
                MString.VARCHAR.instance(20, true),
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(20, true),
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(100, true));
        
        Operator plan = map_NestedLoops(flattenCO, product, 0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
            row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
            row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProductOfTwoOccurrencesOfSameBranch()
    {
        Operator flattenCAOuter =
            flatten_HKeyOrdered(
                filter_Default(
                    groupScan_Default(coi),
                    Arrays.asList(customerRowType, addressRowType)),
                customerRowType,
                addressRowType,
                JoinType.LEFT_JOIN);
        Operator flattenCAInner =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    coi,
                    flattenCAOuter.rowType(),
                    customerRowType,
                    customerRowType,
                    list(addressRowType),
                    InputPreservationOption.KEEP_INPUT,
                    0,
                    lookaheadQuantum()),
                customerRowType,
                addressRowType,
                JoinType.LEFT_JOIN);
        Operator product =
            product_Nested(
                flattenCAInner,
                flattenCAOuter.rowType(),
                customerRowType,
                flattenCAInner.rowType(),
                0);
        RowType productRowType = product.rowType();
        Operator plan = map_NestedLoops(flattenCAOuter, product, 0, pipelineMap(), 1);
        Row[] expected = new Row[]{
            row(productRowType, 1L, "northbridge", 1000L, 1L, "111 1000 st", 1000L, 1L, "111 1000 st"),
            row(productRowType, 1L, "northbridge", 1000L, 1L, "111 1000 st", 1001L, 1L, "111 1001 st"),
            row(productRowType, 1L, "northbridge", 1001L, 1L, "111 1001 st", 1000L, 1L, "111 1000 st"),
            row(productRowType, 1L, "northbridge", 1001L, 1L, "111 1001 st", 1001L, 1L, "111 1001 st"),
            row(productRowType, 2L, "foundation", 2000L, 2L, "222 2000 st", 2000L, 2L, "222 2000 st"),
            row(productRowType, 3L, "matrix", 3000L, 3L, "333 3000 st", 3000, 3L, "333 3000 st"),
            row(productRowType, 3L, "matrix", 3000L, 3L, "333 3000 st", 3001, 3L, "333 3001 st"),
            row(productRowType, 3L, "matrix", 3001L, 3L, "333 3001 st", 3000, 3L, "333 3000 st"),
            row(productRowType, 3L, "matrix", 3001L, 3L, "333 3001 st", 3001, 3L, "333 3001 st"),
            row(productRowType, 4L, "atlas", null, null, null, null, null, null),
            row(productRowType, 5L, "highland", 5000L, 5L, "555 5000 st", 5000, 5L, "555 5000 st"),
            row(productRowType, 5L, "highland", 5000L, 5L, "555 5000 st", 5001, 5L, "555 5001 st"),
            row(productRowType, 5L, "highland", 5001L, 5L, "555 5001 st", 5000, 5L, "555 5000 st"),
            row(productRowType, 5L, "highland", 5001L, 5L, "555 5001 st", 5001, 5L, "555 5001 st"),
            row(productRowType, 6L, "flybridge", null, null, null, null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCursor()
    {
        Operator flattenCO =
            flatten_HKeyOrdered(
                filter_Default(
                    branchLookup_Default(
                        ancestorLookup_Default(
                            indexScan_Default(customerNameIndexRowType, false),
                            coi,
                            customerNameIndexRowType,
                            Collections.singleton(customerRowType),
                            InputPreservationOption.DISCARD_INPUT),
                        coi,
                        customerRowType,
                        orderRowType,
                        InputPreservationOption.KEEP_INPUT),
                    removeDescendentTypes(orderRowType)),
                customerRowType,
                orderRowType,
                INNER_JOIN);
        Operator flattenCA =
            flatten_HKeyOrdered(
                branchLookup_Nested(coi, flattenCO.rowType(), customerRowType, null, list(addressRowType), InputPreservationOption.KEEP_INPUT, 0, lookaheadQuantum()),
                customerRowType,
                addressRowType,
                INNER_JOIN);
        Operator product = product_Nested(flattenCA, flattenCO.rowType(), null, flattenCA.rowType(), 0);
        final RowType coaRowType = product.rowType();
        Operator plan = map_NestedLoops(flattenCO, product, 0, pipelineMap(), 1);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(coaRowType, 2L, "foundation", 200L, 2L, "david", 2000L, 2L, "222 2000 st"),
                    row(coaRowType, 2L, "foundation", 201L, 2L, "david", 2000L, 2L, "222 2000 st"),
                    row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3000L, 3L, "333 3000 st"),
                    row(coaRowType, 3L, "matrix", 300L, 3L, "tom", 3001L, 3L, "333 3001 st"),
                    row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1000L, 1L, "111 1000 st"),
                    row(coaRowType, 1L, "northbridge", 100L, 1L, "ori", 1001L, 1L, "111 1001 st"),
                    row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1000L, 1L, "111 1000 st"),
                    row(coaRowType, 1L, "northbridge", 101L, 1L, "ori", 1001L, 1L, "111 1001 st"),
                };
            }

            @Override
            public boolean reopenTopLevel() {
                return pipelineMap();
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Set<TableRowType> removeDescendentTypes(AisRowType type)
    {
        Set<TableRowType> keepTypes = type.schema().userTableTypes();
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }

    private List<TableRowType> list(TableRowType... rowTypes)
    {
        return Arrays.asList(rowTypes);
    }
}
