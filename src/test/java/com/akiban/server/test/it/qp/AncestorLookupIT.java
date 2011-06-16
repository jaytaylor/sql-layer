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
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.akiban.qp.physicaloperator.API.*;

public class AncestorLookupIT extends PhysicalOperatorITBase
{
    @Before
    public void before()
    {
        super.before();
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
                               true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDescendentIsNotAncestor()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerRowType,
                               list(itemRowType),
                               true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelfIsNotAncestor()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerRowType,
                               list(customerRowType),
                               true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeepIndexInput()
    {
        ancestorLookup_Default(groupScan_Default(coi),
                               coi,
                               customerNameIndexRowType,
                               list(customerRowType),
                               true);
    }

    // Test ancestor lookup given index row

    @Test
    public void testItemIndexToMissingCustomerAndOrder()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(999, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerAndOrder()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(111, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToCustomerOnly()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(111, customerRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemIndexToOrderOnly()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(111, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerAndOrder()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(311, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToCustomerOnly()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(311, customerRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToOrderOnly()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(311, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemIndexToItem()
    {
        PhysicalOperator plan = indexRowToAncestorPlan(311, itemRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
    }

    // Test ancestor lookup given group row

    @Test
    public void testItemRowToMissingCustomerAndOrder()
    {
        PhysicalOperator plan = groupRowToAncestorPlan(999, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToCustomerAndOrder()
    {
        // Keep input
        PhysicalOperator plan = groupRowToAncestorPlan(111, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType, orderRowType);
        cursor = cursor(plan, adapter);
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
        PhysicalOperator plan = groupRowToAncestorPlan(111, true, customerRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, customerRowType);
        cursor = cursor(plan, adapter);
        expected = new RowBase[]{
            row(customerRowType, 1L, "northbridge")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testItemRowToOrderOnly()
    {
        // Keep input
        PhysicalOperator plan = groupRowToAncestorPlan(111, true, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori"),
            row(itemRowType, 111L, 11L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(111, false, orderRowType);
        cursor = cursor(plan, adapter);
        expected = new RowBase[]{
            row(orderRowType, 11L, 1L, "ori")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerAndOrder()
    {
        // Keep input
        PhysicalOperator plan = groupRowToAncestorPlan(311, true, customerRowType, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType, orderRowType);
        cursor = cursor(plan, adapter);
        expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToCustomerOnly()
    {
        // Keep input
        PhysicalOperator plan = groupRowToAncestorPlan(311, true, customerRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, customerRowType);
        cursor = cursor(plan, adapter);
        expected = new RowBase[]{
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testOrphanItemRowToOrderOnly()
    {
        // Keep input
        PhysicalOperator plan = groupRowToAncestorPlan(311, true, orderRowType);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter"),
            row(itemRowType, 311L, 31L)
        };
        compareRows(expected, cursor);
        // Don't keep input
        plan = groupRowToAncestorPlan(311, false, orderRowType);
        cursor = cursor(plan, adapter);
        expected = new RowBase[]{
            row(orderRowType, 31L, 3L, "peter")
        };
        compareRows(expected, cursor);
    }

    // For use by this class

    private PhysicalOperator indexRowToAncestorPlan(int iid, RowType ... rowTypes)
    {
        return
            ancestorLookup_Default
                (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                 coi,
                 itemIidIndexRowType,
                 list(rowTypes),
                 false);
    }

    private PhysicalOperator groupRowToAncestorPlan(int iid, boolean keepInput, RowType ... rowTypes)
    {
        return
            ancestorLookup_Default
                (branchLookup_Default
                        (indexScan_Default(itemIidIndexRowType, false, itemIidEQ(iid)),
                                coi,
                                itemIidIndexRowType,
                                itemRowType,
                                false),
                 coi,
                 itemRowType,
                 list(rowTypes),
                 keepInput);
    }

    private IndexKeyRange itemIidEQ(int iid)
    {
        IndexBound bound = itemIidIndexBound(iid);
        return new IndexKeyRange(bound, true, bound, true);
    }

    private IndexBound itemIidIndexBound(int iid)
    {
        return new IndexBound(row(itemRowType, iid, null), new SetColumnSelector(0));
    }

    private List<RowType> list(RowType... rowTypes)
    {
        return Arrays.asList(rowTypes);
    }
}
