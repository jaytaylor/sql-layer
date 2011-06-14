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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.physicaloperator.API.*;

public class BranchLookupIT extends PhysicalOperatorITBase
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
            createNewRow(address, 1002L, 1L, "111 2222 st"),
            createNewRow(address, 2001L, 2L, "222 1111 st"),
            createNewRow(address, 2002L, 2L, "222 2222 st"),
            createNewRow(address, 4001L, 4L, "444 1111 st"),
            createNewRow(address, 4002L, 4L, "444 2222 st"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, "jack"),
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
            createNewRow(address, 5001, 5L, "555 1111 st"),
            createNewRow(item, 311L, 31L),
            createNewRow(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullInputRowType()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                null,
                customerRowType,
                true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullOutputRowType()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerRowType,
                null,
                true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupSelf()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerRowType,
                customerRowType,
                true);
    }

    @Test
    public void testLookupSelfFromIndex()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerNameIndexRowType,
                customerRowType,
                false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepIndexInput()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerNameIndexRowType,
                customerRowType,
                true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBranchNonRootLookup()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                addressRowType,
                itemRowType,
                true);
    }

    @Test
    public void testBranchRootLookup()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                addressRowType,
                orderRowType,
                true);
    }

    // customer index -> customer + descendents

    @Test
    public void testCustomerIndexToMissingCustomer()
    {
        PhysicalOperator plan = customerNameToCustomerPlan("matrix");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomerIndexToCustomer()
    {
        PhysicalOperator plan = customerNameToCustomerPlan("northbridge");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
        PhysicalOperator plan = customerNameToCustomerPlan("highland");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 4L, "highland"),
            row(addressRowType, 4001L, 4L, "444 1111 st"),
            row(addressRowType, 4002L, 4L, "444 2222 st")
        };
        compareRows(expected, cursor);
    }

    // address index -> customer + descendents

    @Test
    public void testAddressIndexToMissingCustomer()
    {
        PhysicalOperator plan = addressAddressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAddressIndexToCustomer()
    {
        PhysicalOperator plan = addressAddressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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

    @Test
    public void testAddressToMissingCustomer()
    {
        PhysicalOperator plan = addressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAddressToCustomer()
    {
        PhysicalOperator plan = addressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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

    @Test
    public void testAddressToOrder()
    {
        PhysicalOperator plan = addressToOrderPlan("222 2222 st", false);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(itemRowType, 211L, 21L),
            row(itemRowType, 212L, 21L),
            row(orderRowType, 22L, 2L, "jack"),
            row(itemRowType, 221L, 22L),
            row(itemRowType, 222L, 22L)
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAddressToMissingOrder()
    {
        PhysicalOperator plan = addressToOrderPlan("444 2222 st", false);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    // Ordering of input row relative to branch

    @Test
    public void testAddressToOrderAndAddress()
    {
        PhysicalOperator plan = addressToOrderPlan("222 2222 st", true);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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

    @Test
    public void testOrderToOrderAndAddress()
    {
        PhysicalOperator plan = orderToAddressPlan("tom", true);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 21L, 2L, "tom"),
            row(addressRowType, 2001L, 2L, "222 1111 st"),
            row(addressRowType, 2002L, 2L, "222 2222 st")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemToItemAndAddress()
    {
        PhysicalOperator plan = itemToAddressPlan(111L, true);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 111L, 11L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
        };
        compareRows(expected, cursor);
    }

    // For use by this class

    private PhysicalOperator customerNameToCustomerPlan(String customerName)
    {
        return
            branchLookup_Default(
                indexScan_Default(customerNameIndexRowType, false, customerNameEQ(customerName)),
                coi,
                customerNameIndexRowType,
                customerRowType,
                false);
    }

    private PhysicalOperator addressAddressToCustomerPlan(String address)
    {
        return
            branchLookup_Default(
                indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                coi,
                addressAddressIndexRowType,
                customerRowType,
                false);
    }

    private PhysicalOperator addressToCustomerPlan(String address)
    {
        return
            branchLookup_Default(
                    branchLookup_Default(
                        indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                        coi,
                        addressAddressIndexRowType,
                        addressRowType,
                        false),
                    coi,
                    addressRowType,
                    customerRowType,
                    false);
    }

    private PhysicalOperator addressToOrderPlan(String address, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                    coi,
                    addressAddressIndexRowType,
                    Arrays.asList(addressRowType),
                    false),
                coi,
                addressRowType,
                orderRowType,
                keepInput);
    }

    private PhysicalOperator orderToAddressPlan(String salesman, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ(salesman)),
                    coi,
                    orderSalesmanIndexRowType,
                    Arrays.asList(orderRowType),
                    false),
                coi,
                orderRowType,
                addressRowType,
                keepInput);
    }

    private PhysicalOperator itemToAddressPlan(long iid, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                    coi,
                    itemIidIndexRowType,
                    Arrays.asList(itemRowType),
                    false),
                coi,
                itemRowType,
                addressRowType,
                keepInput);
    }

    private IndexKeyRange customerNameEQ(String name)
    {
        IndexBound bound = customerNameIndexBound(name);
        return new IndexKeyRange(bound, true, bound, true);
    }

    private IndexKeyRange addressAddressEQ(String address)
    {
        IndexBound bound = addressAddressIndexBound(address);
        return new IndexKeyRange(bound, true, bound, true);
    }

    private IndexKeyRange orderSalesmanEQ(String salesman)
    {
        IndexBound bound = orderSalesmanIndexBound(salesman);
        return new IndexKeyRange(bound, true, bound, true);
    }

    private IndexKeyRange itemIidEQ(long iid)
    {
        IndexBound bound = itemIidIndexBound(iid);
        return new IndexKeyRange(bound, true, bound, true);
    }

    private IndexBound customerNameIndexBound(String name)
    {
        return new IndexBound(userTable(customer), row(customerRowType, null, name), new SetColumnSelector(1));
    }

    private IndexBound addressAddressIndexBound(String addr)
    {
        return new IndexBound(userTable(address), row(addressRowType, null, null, addr), new SetColumnSelector(2));
    }

    private IndexBound orderSalesmanIndexBound(String salesman)
    {
        return new IndexBound(userTable(order), row(orderRowType, null, null, salesman), new SetColumnSelector(2));
    }

    private IndexBound itemIidIndexBound(long iid)
    {
        return new IndexBound(userTable(item), row(itemRowType, iid, null), new SetColumnSelector(0));
    }
}
