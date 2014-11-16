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
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;

public class GroupLookup_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        Row[] dbWithOrphans = new Row[]{
            row(customer, 1L, "northbridge"),
            row(customer, 2L, "foundation"),
            row(customer, 4L, "highland"),
            row(address, 1001L, 1L, "111 1111 st"),
            row(address, 1002L, 1L, "111 2222 st"),
            row(address, 2001L, 2L, "222 1111 st"),
            row(address, 2002L, 2L, "222 2222 st"),
            row(address, 4001L, 4L, "444 1111 st"),
            row(address, 4002L, 4L, "444 2222 st"),
            row(order, 11L, 1L, "ori"),
            row(order, 12L, 1L, "david"),
            row(order, 21L, 2L, "tom"),
            row(order, 22L, 2L, "jack"),
            row(order, 31L, 3L, "peter"),
            row(item, 111L, 11L),
            row(item, 112L, 11L),
            row(item, 121L, 12L),
            row(item, 122L, 12L),
            row(item, 211L, 21L),
            row(item, 212L, 21L),
            row(item, 221L, 22L),
            row(item, 222L, 22L),
            // orphans
            row(address, 5001L, 5L, "555 1111 st"),
            row(item, 311L, 31L),
            row(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    protected int lookaheadQuantum() {
        return 1;
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testAtLeastOneAncestor()
    {
        groupLookup_Default(groupScan_Default(coi),
                            coi,
                            customerRowType,
                            list(),
                            InputPreservationOption.KEEP_INPUT,
                            lookaheadQuantum());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepInputSelf()
    {
        groupLookup_Default(groupScan_Default(coi),
                            coi,
                            customerRowType,
                            list(customerRowType),
                            InputPreservationOption.KEEP_INPUT,
                            lookaheadQuantum());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepIndexInput()
    {
        groupLookup_Default(groupScan_Default(coi),
                            coi,
                            customerNameIndexRowType,
                            list(customerRowType),
                            InputPreservationOption.KEEP_INPUT,
                            lookaheadQuantum());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBranchNonRootLookup()
    {
        groupLookup_Default(groupScan_Default(coi),
                            coi,
                            addressRowType,
                            list(itemRowType),
                            InputPreservationOption.KEEP_INPUT,
                            lookaheadQuantum());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBranchRootLookup()
    {
        groupLookup_Default(groupScan_Default(coi),
                            coi,
                            addressRowType,
                            list(orderRowType, itemRowType),
                            InputPreservationOption.KEEP_INPUT,
                            lookaheadQuantum());
    }

    // Test ancestor lookup given index row

    @Test
    public void testItemIndexToMissingCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(999, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(111, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerOnly()
    {
        Operator plan = indexRowToAncestorPlan(111, customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToOrderOnly()
    {
        Operator plan = indexRowToAncestorPlan(111, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(311, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerOnly()
    {
        Operator plan = indexRowToAncestorPlan(311, customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToOrderOnly()
    {
        Operator plan = indexRowToAncestorPlan(311, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToItem()
    {
        Operator plan = indexRowToAncestorPlan(311, itemRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
    }

    // Test ancestor lookup given group row

    @Test
    public void testItemRowToMissingCustomerAndOrder()
    {
        Operator plan = groupRowToAncestorPlan(999, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToCustomerAndOrder()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(111, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType, orderRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToCustomerOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(111, true, customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToOrderOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(111, true, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, orderRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerAndOrder()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType, orderRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, customerRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToOrderOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, orderRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, orderRowType);
        cursor = cursor(plan, queryContext, queryBindings);
        expected = new Row[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }
    
    // hkey input
    
    @Test
    public void testOrderHKeyToCustomerAndOrder()
    {
        Operator plan = orderHKeyToCustomerAndOrderPlan("jack");
        Row[] expected = new Row[]{
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    // customer index -> customer + descendents

    @Test
    public void testCustomerIndexToMissingCustomer()
    {
        Operator plan = customerNameToCustomerPlan("matrix");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomerIndexToCustomer()
    {
        Operator plan = customerNameToCustomerPlan("northbridge");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomerIndexToCustomerWithNoOrders()
    {
        Operator plan = customerNameToCustomerPlan("highland");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 4L, "highland"),
            row(addressRowType, 4001L, 4L, "444 1111 st"),
            row(addressRowType, 4002L, 4L, "444 2222 st")
        };
        compareRows(expected, cursor);
    }

    // address index -> customer + descendents
    // MOSTLY IGNORE since not supported by GroupLookup_Default currently.

    @Test @Ignore
    public void testAddressIndexToMissingCustomer()
    {
        Operator plan = addressAddressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test @Ignore
    public void testAddressIndexToCustomer()
    {
        Operator plan = addressAddressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    // address row -> customer + descendents

    @Test @Ignore
    public void testAddressToMissingCustomer()
    {
        Operator plan = addressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test @Ignore
    public void testAddressToCustomer()
    {
        Operator plan = addressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    // address row -> order + descendents

    @Test @Ignore
    public void testAddressToOrder()
    {
        Operator plan = addressToOrderPlan("222 2222 st", false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L)
        };
        compareRows(expected, cursor);
    }

    @Test @Ignore
    public void testAddressToMissingOrder()
    {
        Operator plan = addressToOrderPlan("444 2222 st", false);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor);
    }

    // Ordering of input row relative to branch

    @Test @Ignore
    public void testAddressToOrderAndAddress()
    {
        Operator plan = addressToOrderPlan("222 2222 st", true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L),
            row(addressRowType, 2002L, 2L, "222 2222 st"),
        };
        compareRows(expected, cursor);
    }

    @Test @Ignore
    public void testOrderToOrderAndAddress()
    {
        Operator plan = orderToAddressPlan("tom", true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    @Test @Ignore
    public void testItemToItemAndAddress()
    {
        Operator plan = itemToAddressPlan(111L, true);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(itemRowType, 111L, 11L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderToOrderAndCustomerAndItems()
    {
        Operator plan = 
            groupLookup_Default(
                indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ("ori")),
                coi,
                orderSalesmanIndexRowType,
                list(orderRowType, customerRowType, itemRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomerToOrderOnly()
    {
        Operator plan =
            groupLookup_Default(
                indexScan_Default(customerNameIndexRowType, false, customerNameEQ("foundation")),
                coi,
                customerNameIndexRowType,
                list(orderRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrderUnionToOrder()
    {
        Operator orderOrItem =
            hKeyUnion_Ordered(
                indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ("ori")),
                indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ("david")),
                orderSalesmanIndexRowType,
                orderSalesmanIndexRowType,
                2,
                2,
                1,
                customerRowType);
        Operator plan =
            groupLookup_Default(
                orderOrItem,
                coi,
                orderOrItem.rowType(),
                descendants(orderRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L),
            row(itemRowType, 112L, 11L),
            row(orderRowType, 12L, 1L, "david"),
            row(itemRowType, 121L, 12L),
            row(itemRowType, 122L, 12L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testPKAccess() {
        Operator plan = 
                groupLookup_Default(
                        indexScan_Default(indexType(order, "oid"), false, orderIdEQ(11L)),
                        coi,
                        indexType(order, "oid"),
                        descendants(orderRowType),
                        InputPreservationOption.DISCARD_INPUT,
                        lookaheadQuantum());
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
                row(orderRowType, 11L, 1L, "ori"),
                row(itemRowType, 111L, 11L),
                row(itemRowType, 112L, 11L),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            groupLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                };
            }

            @Override
            public boolean reopenTopLevel() {
                return (lookaheadQuantum() > 1);
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // For use by this class

    private Operator indexRowToAncestorPlan(int iid, TableRowType... rowTypes)
    {
        return
            groupLookup_Default
                (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                 coi,
                 itemIidIndexRowType,
                 list(rowTypes),
                 InputPreservationOption.DISCARD_INPUT,
                 lookaheadQuantum());
    }

    private Operator groupRowToAncestorPlan(int iid, boolean keepInput, TableRowType... rowTypes)
    {
        return
            groupLookup_Default
                (groupLookup_Default
                     (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                      coi,
                      itemIidIndexRowType,
                      list(itemRowType),
                      InputPreservationOption.DISCARD_INPUT,
                      lookaheadQuantum()),
                 coi,
                 itemRowType,
                 list(rowTypes),
                 keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT,
                 lookaheadQuantum());
    }

    private Operator orderHKeyToCustomerAndOrderPlan(String salesman)
    {
        Operator indexMerge = hKeyUnion_Ordered(
            indexScan_Default(orderSalesmanIndexRowType, false, salesmanEQ(salesman)),
            indexScan_Default(orderSalesmanIndexRowType, false, salesmanEQ(salesman)),
            orderSalesmanIndexRowType,
            orderSalesmanIndexRowType,
            2,
            2,
            2,
            orderRowType);
        return
            groupLookup_Default(
                indexMerge,
                coi,
                indexMerge.rowType(),
                Arrays.asList(customerRowType, orderRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private Operator customerNameToCustomerPlan(String customerName)
    {
        return
            groupLookup_Default(
                indexScan_Default(customerNameIndexRowType, false, customerNameEQ(customerName)),
                coi,
                customerNameIndexRowType,
                descendants(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private Operator addressAddressToCustomerPlan(String address)
    {
        return
            groupLookup_Default(
                indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                coi,
                addressAddressIndexRowType,
                descendants(customerRowType),
                InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private Operator addressToCustomerPlan(String address)
    {
        return
            groupLookup_Default(
                    branchLookup_Default(
                        indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                        coi,
                        addressAddressIndexRowType,
                        addressRowType,
                        InputPreservationOption.DISCARD_INPUT),
                    coi,
                    addressRowType,
                    descendants(customerRowType),
                    InputPreservationOption.DISCARD_INPUT,
                    lookaheadQuantum());
    }

    private Operator addressToOrderPlan(String address, boolean keepInput)
    {
        return
            groupLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                    coi,
                    addressAddressIndexRowType,
                    Arrays.asList(addressRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                addressRowType,
                descendants(orderRowType),
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private Operator orderToAddressPlan(String salesman, boolean keepInput)
    {
        return
            groupLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ(salesman)),
                    coi,
                    orderSalesmanIndexRowType,
                    Arrays.asList(orderRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                orderRowType,
                descendants(addressRowType),
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private Operator itemToAddressPlan(long iid, boolean keepInput)
    {
        return
            groupLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                    coi,
                    itemIidIndexRowType,
                    Arrays.asList(itemRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                itemRowType,
                descendants(addressRowType),
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT,
                lookaheadQuantum());
    }

    private IndexKeyRange itemIidEQ(int iid)
    {
        IndexBound bound = itemIidIndexBound(iid);
        return IndexKeyRange.bounded(itemIidIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange salesmanEQ(String salesman)
    {
        IndexBound bound = orderSalesmanBound(salesman);
        return IndexKeyRange.bounded(orderSalesmanIndexRowType, bound, true, bound, true);
    }

    private IndexBound itemIidIndexBound(int iid)
    {
        return new IndexBound(row(itemIidIndexRowType, iid), new SetColumnSelector(0));
    }

    private IndexBound orderSalesmanBound(String salesman)
    {
        return new IndexBound(row(orderSalesmanIndexRowType, salesman), new SetColumnSelector(0));
    }

    private IndexKeyRange customerNameEQ(String name)
    {
        IndexBound bound = customerNameIndexBound(name);
        return IndexKeyRange.bounded(customerNameIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange addressAddressEQ(String address)
    {
        IndexBound bound = addressAddressIndexBound(address);
        return IndexKeyRange.bounded(addressAddressIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange orderSalesmanEQ(String salesman)
    {
        IndexBound bound = orderSalesmanIndexBound(salesman);
        return IndexKeyRange.bounded(orderSalesmanIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange orderIdEQ(long oid) 
    {
        IndexRowType indexRowType = indexType(order, "oid");
        IndexBound bound = new IndexBound(row(indexRowType, oid), new SetColumnSelector(0));
        return IndexKeyRange.bounded(indexRowType, bound, true, bound, true);
        
    }
    private IndexKeyRange itemIidEQ(long iid)
    {
        IndexBound bound = itemIidIndexBound(iid);
        return IndexKeyRange.bounded(itemIidIndexRowType, bound, true, bound, true);
    }

    private IndexBound customerNameIndexBound(String name)
    {
        return new IndexBound(row(customerNameIndexRowType, name), new SetColumnSelector(0));
    }

    private IndexBound addressAddressIndexBound(String addr)
    {
        return new IndexBound(row(addressAddressIndexRowType, addr), new SetColumnSelector(0));
    }

    private IndexBound orderSalesmanIndexBound(String salesman)
    {
        return new IndexBound(row(orderSalesmanIndexRowType, salesman), new SetColumnSelector(0));
    }

    private IndexBound itemIidIndexBound(long iid)
    {
        return new IndexBound(row(itemIidIndexRowType, iid), new SetColumnSelector(0));
    }

    private List<TableRowType> descendants(TableRowType root)
    {
        List<TableRowType> result = new ArrayList<>();
        result.add(root);
        for (RowType rowType : Schema.descendentTypes(root, schema.userTableTypes())) {
            result.add((TableRowType)rowType);
        }
        return result;
    }

    private List<TableRowType> list(TableRowType... rowTypes)
    {
        return Arrays.asList(rowTypes);
    }
}
