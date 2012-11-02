/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class BranchLookup_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
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
            createNewRow(address, 5001L, 5L, "555 1111 st"),
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
                InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullOutputRowType()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerRowType,
                null,
                InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupSelf()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerRowType,
                customerRowType,
                InputPreservationOption.KEEP_INPUT);
    }

    @Test
    public void testLookupSelfFromIndex()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerNameIndexRowType,
                customerRowType,
                InputPreservationOption.DISCARD_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepIndexInput()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                customerNameIndexRowType,
                customerRowType,
                InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBranchNonRootLookup()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                addressRowType,
                itemRowType,
                InputPreservationOption.KEEP_INPUT);
    }

    @Test
    public void testBranchRootLookup()
    {
        branchLookup_Default(groupScan_Default(coi),
                coi,
                addressRowType,
                orderRowType,
                InputPreservationOption.KEEP_INPUT);
    }

    // customer index -> customer + descendents

    @Test
    public void testCustomerIndexToMissingCustomer()
    {
        Operator plan = customerNameToCustomerPlan("matrix");
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testCustomerIndexToCustomer()
    {
        Operator plan = customerNameToCustomerPlan("northbridge");
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = customerNameToCustomerPlan("highland");
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = addressAddressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAddressIndexToCustomer()
    {
        Operator plan = addressAddressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = addressToCustomerPlan("555 1111 st");
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(addressRowType, 5001L, 5L, "555 1111 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAddressToCustomer()
    {
        Operator plan = addressToCustomerPlan("222 2222 st");
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = addressToOrderPlan("222 2222 st", false);
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = addressToOrderPlan("444 2222 st", false);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    // Ordering of input row relative to branch

    @Test
    public void testAddressToOrderAndAddress()
    {
        Operator plan = addressToOrderPlan("222 2222 st", true);
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = orderToAddressPlan("tom", true);
        Cursor cursor = cursor(plan, queryContext);
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
        Operator plan = itemToAddressPlan(111L, true);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 111L, 11L),
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
        };
        compareRows(expected, cursor);
    }
    
    @Test
    public void testOrderUnionToAddress()
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
            branchLookup_Default(
                orderOrItem,
                coi,
                orderOrItem.rowType(),
                addressRowType,
                InputPreservationOption.DISCARD_INPUT);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(addressRowType, 1001L, 1L, "111 1111 st"),
            row(addressRowType, 1002L, 1L, "111 2222 st"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            branchLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(customerRowType)),
                coi,
                customerRowType,
                addressRowType,
                InputPreservationOption.DISCARD_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(addressRowType, 1001L, 1L, "111 1111 st"),
                    row(addressRowType, 1002L, 1L, "111 2222 st"),
                    row(addressRowType, 2001L, 2L, "222 1111 st"),
                    row(addressRowType, 2002L, 2L, "222 2222 st"),
                    row(addressRowType, 4001L, 4L, "444 1111 st"),
                    row(addressRowType, 4002L, 4L, "444 2222 st"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // For use by this class

    private Operator customerNameToCustomerPlan(String customerName)
    {
        return
            branchLookup_Default(
                indexScan_Default(customerNameIndexRowType, false, customerNameEQ(customerName)),
                coi,
                customerNameIndexRowType,
                customerRowType,
                InputPreservationOption.DISCARD_INPUT);
    }

    private Operator addressAddressToCustomerPlan(String address)
    {
        return
            branchLookup_Default(
                indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                coi,
                addressAddressIndexRowType,
                customerRowType,
                InputPreservationOption.DISCARD_INPUT);
    }

    private Operator addressToCustomerPlan(String address)
    {
        return
            branchLookup_Default(
                    branchLookup_Default(
                        indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                        coi,
                        addressAddressIndexRowType,
                        addressRowType,
                        InputPreservationOption.DISCARD_INPUT),
                    coi,
                    addressRowType,
                    customerRowType,
                    InputPreservationOption.DISCARD_INPUT);
    }

    private Operator addressToOrderPlan(String address, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(addressAddressIndexRowType, false, addressAddressEQ(address)),
                    coi,
                    addressAddressIndexRowType,
                    Arrays.asList(addressRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                addressRowType,
                orderRowType,
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT);
    }

    private Operator orderToAddressPlan(String salesman, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(orderSalesmanIndexRowType, false, orderSalesmanEQ(salesman)),
                    coi,
                    orderSalesmanIndexRowType,
                    Arrays.asList(orderRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                orderRowType,
                addressRowType,
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT);
    }

    private Operator itemToAddressPlan(long iid, boolean keepInput)
    {
        return
            branchLookup_Default(
                ancestorLookup_Default(
                    indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                    coi,
                    itemIidIndexRowType,
                    Arrays.asList(itemRowType),
                    InputPreservationOption.DISCARD_INPUT),
                coi,
                itemRowType,
                addressRowType,
                keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT);
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
}
