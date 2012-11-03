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
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.*;

public class AncestorLookup_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();
        NewRow[] dbWithOrphans = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
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
            createNewRow(item, 311L, 31L),
            createNewRow(item, 312L, 31L)};
        use(dbWithOrphans);
    }

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testAtLeastOneAncestor()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerRowType,
                               list(),
                               InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDescendentIsNotAncestor()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerRowType,
                               list(itemRowType),
                               InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelfIsNotAncestor()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerRowType,
                               list(customerRowType),
                               InputPreservationOption.KEEP_INPUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepIndexInput()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerNameIndexRowType,
                               list(customerRowType),
                               InputPreservationOption.KEEP_INPUT);
    }

    // Test ancestor lookup given index row

    @Test
    public void testItemIndexToMissingCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(999, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(111, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerOnly()
    {
        Operator plan = indexRowToAncestorPlan(111, customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToOrderOnly()
    {
        Operator plan = indexRowToAncestorPlan(111, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerAndOrder()
    {
        Operator plan = indexRowToAncestorPlan(311, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerOnly()
    {
        Operator plan = indexRowToAncestorPlan(311, customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToOrderOnly()
    {
        Operator plan = indexRowToAncestorPlan(311, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToItem()
    {
        Operator plan = indexRowToAncestorPlan(311, itemRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
    }

    // Test ancestor lookup given group row

    @Test
    public void testItemRowToMissingCustomerAndOrder()
    {
        Operator plan = groupRowToAncestorPlan(999, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToCustomerAndOrder()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(111, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType, orderRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
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
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToOrderOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(111, true, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, orderRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerAndOrder()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType, orderRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, customerRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToOrderOnly()
    {
        // Keep input
        Operator plan = groupRowToAncestorPlan(311, true, orderRowType);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, orderRowType);
        cursor = cursor(plan, queryContext);
        expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }
    
    // hkey input
    
    @Test
    public void testOrderHKeyToCustomerAndOrder()
    {
        Operator plan = orderHKeyToCustomerAndOrderPlan("jack");
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 2L, "foundation"),
            row(orderRowType, 22L, 2L, "jack"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            ancestorLookup_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(orderRowType)),
                coi,
                orderRowType,
                Collections.singleton(customerRowType),
                InputPreservationOption.DISCARD_INPUT);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 1L, "northbridge"),
                    row(customerRowType, 2L, "foundation"),
                    row(customerRowType, 2L, "foundation"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // For use by this class

    private Operator indexRowToAncestorPlan(int iid, UserTableRowType ... rowTypes)
    {
        return
            ancestorLookup_Default
                (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                 coi,
                 itemIidIndexRowType,
                 list(rowTypes),
                 InputPreservationOption.DISCARD_INPUT);
    }

    private Operator groupRowToAncestorPlan(int iid, boolean keepInput, UserTableRowType ... rowTypes)
    {
        return
            ancestorLookup_Default
                (branchLookup_Default
                     (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                      coi,
                      itemIidIndexRowType,
                      itemRowType,
                      InputPreservationOption.DISCARD_INPUT),
                 coi,
                 itemRowType,
                 list(rowTypes),
                 keepInput ? InputPreservationOption.KEEP_INPUT : InputPreservationOption.DISCARD_INPUT);
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
            ancestorLookup_Default(
                indexMerge,
                coi,
                indexMerge.rowType(),
                Arrays.asList(customerRowType, orderRowType),
                InputPreservationOption.DISCARD_INPUT);
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

    private List<UserTableRowType> list(UserTableRowType... rowTypes)
    {
        return Arrays.asList(rowTypes);
    }
}
