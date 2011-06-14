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
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.expression.API.field;
import static com.akiban.qp.physicaloperator.API.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RunIdPropagationIT extends PhysicalOperatorITBase
{
    @Before
    public void before()
    {
        super.before();
        NewRow[] db = new NewRow[]{
            createNewRow(customer, 1L, "northbridge"),
            createNewRow(customer, 2L, "foundation"),
            createNewRow(order, 11L, 1L, "ori"),
            createNewRow(order, 12L, 1L, "david"),
            createNewRow(order, 21L, 2L, "tom"),
            createNewRow(order, 22L, 2L, "jack"),
            createNewRow(item, 111L, 11L),
            createNewRow(item, 112L, 11L),
            createNewRow(item, 121L, 12L),
            createNewRow(item, 122L, 12L),
            createNewRow(item, 211L, 21L),
            createNewRow(item, 212L, 21L),
            createNewRow(item, 221L, 22L),
            createNewRow(item, 222L, 22L)
        };
        use(db);
    }

    @Test
    public void testIndexScan()
    {
        PhysicalOperator plan = indexScan_Default(orderSalesmanIndexRowType, false, null);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            RowBase row = cursor.currentRow();
            assertEquals(expectedRunId++, row.runId());
        }
        assertEquals(4, expectedRunId);
    }

    @Test
    public void testAncestorLookupAfterIndexScan()
    {
        PhysicalOperator plan =
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType, false, null),
                coi,
                itemIidIndexRowType,
                Arrays.asList(customerRowType, orderRowType, itemRowType),
                false);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            // customer
            RowBase row = cursor.currentRow();
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // order
            assertTrue(cursor.next());
            row = cursor.currentRow();
            assertEquals(orderRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // item
            assertTrue(cursor.next());
            row = cursor.currentRow();
            assertEquals(itemRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // done with this run
            expectedRunId++;
        }
        assertEquals(8, expectedRunId);
    }

    @Test
    public void testBranchLookupAfterIndexScan()
    {
        PhysicalOperator plan =
            branchLookup_Default(
                indexScan_Default(itemIidIndexRowType, false, null),
                coi,
                itemIidIndexRowType,
                customerRowType,
                false);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            // customer
            RowBase row = cursor.currentRow();
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // each customer has 2 orders
            for (int o = 0; o < 2; o++) {
                assertTrue(cursor.next());
                row = cursor.currentRow();
                assertEquals(orderRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
                // each order has 2 items
                for (int i = 0; i < 2; i++) {
                    assertTrue(cursor.next());
                    row = cursor.currentRow();
                    assertEquals(itemRowType, row.rowType());
                    assertEquals(expectedRunId, row.runId());
                }
            }
            // done with this run
            expectedRunId++;
        }
        assertEquals(8, expectedRunId);
    }

    @Test
    public void testCutAfterIndexScan()
    {
        PhysicalOperator plan =
            cut_Default(
                branchLookup_Default(
                    indexScan_Default(customerNameIndexRowType, false, null),
                    coi,
                    customerNameIndexRowType,
                    customerRowType,
                    false),
                customerRowType);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            RowBase row = cursor.currentRow();
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId++, row.runId());
        }
        assertEquals(2, expectedRunId);
    }

    @Test
    public void testExtractAfterIndexScan()
    {
        PhysicalOperator plan =
            extract_Default(
                branchLookup_Default(
                    indexScan_Default(customerNameIndexRowType, false, null),
                    coi,
                    customerNameIndexRowType,
                    customerRowType,
                    false),
                Arrays.asList(orderRowType));
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            // Each customer has 2 orders
            for (int o = 0; o < 2; o++) {
                if (o > 0) {
                    assertTrue(cursor.next());
                }
                RowBase row = cursor.currentRow();
                assertEquals(orderRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
                // Each order has 2 items
                for (int i = 0; i < 2; i++) {
                    assertTrue(cursor.next());
                    row = cursor.currentRow();
                    assertEquals(itemRowType, row.rowType());
                    assertEquals(expectedRunId, row.runId());
                }
            }
            // done with this run
            expectedRunId++;
        }
        assertEquals(2, expectedRunId);
    }

    @Test
    public void testFlattenAfterIndexScan()
    {
        PhysicalOperator plan =
            flatten_HKeyOrdered(
                cut_Default(
                    branchLookup_Default(
                        indexScan_Default(customerNameIndexRowType, false, null),
                        coi,
                        customerNameIndexRowType,
                        customerRowType,
                        false),
                    orderRowType),
                customerRowType,
                orderRowType,
                DEFAULT);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            // There should be 2 co rows per run
            for (int co = 0; co < 2; co++) {
                if (co > 0) {
                    assertTrue(cursor.next());
                }
                RowBase row = cursor.currentRow();
                assertEquals(coRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
            }
            // done with this run
            expectedRunId++;
        }
        assertEquals(2, expectedRunId);
    }

    @Test
    public void testProjectAfterIndexScan()
    {
        PhysicalOperator plan =
            project_Default(
                cut_Default(
                    branchLookup_Default(
                        indexScan_Default(customerNameIndexRowType, false, null),
                        coi,
                        customerNameIndexRowType,
                        customerRowType,
                        false),
                    customerRowType),
                customerRowType,
                Arrays.asList(field(1)));
        RowType customerNameRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        while (cursor.next()) {
            // There should be 1 projected rows per run
            for (int p = 0; p < 1; p++) {
                if (p > 0) {
                    assertTrue(cursor.next());
                }
                RowBase row = cursor.currentRow();
                assertEquals(customerNameRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
            }
            // done with this run
            expectedRunId++;
        }
        assertEquals(2, expectedRunId);
    }

    @Test
    public void testGroupScan()
    {
        PhysicalOperator plan = groupScan_Default(coi);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int count = 0;
        while (cursor.next()) {
            RowBase row = cursor.currentRow();
            assertEquals(-1, row.runId());
            count++;
        }
        assertEquals(14, count);
    }
}
