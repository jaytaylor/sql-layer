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
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.AisRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static com.akiban.server.expression.std.Expressions.field;
import static com.akiban.qp.operator.API.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RunIdPropagationIT extends OperatorITBase
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
        Operator plan = indexScan_Default(orderSalesmanIndexRowType, false);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            assertEquals(expectedRunId++, row.runId());
        }
        assertEquals(4, expectedRunId);
    }

    @Test
    public void testAncestorLookupAfterIndexScan()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType, false),
                coi,
                itemIidIndexRowType,
                Arrays.asList(customerRowType, orderRowType, itemRowType),
                LookupOption.DISCARD_INPUT);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            // customer
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // order
            assertTrue((row = cursor.next()) != null);
            assertEquals(orderRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // item
            assertTrue((row = cursor.next()) != null);
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
        Operator plan =
            branchLookup_Default(
                indexScan_Default(itemIidIndexRowType, false),
                coi,
                itemIidIndexRowType,
                customerRowType,
                LookupOption.DISCARD_INPUT);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            // customer
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId, row.runId());
            // each customer has 2 orders
            for (int o = 0; o < 2; o++) {
                assertTrue((row = cursor.next()) != null);
                assertEquals(orderRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
                // each order has 2 items
                for (int i = 0; i < 2; i++) {
                    assertTrue((row = cursor.next()) != null);
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
        Operator plan =
            filter_Default(
                branchLookup_Default(
                    indexScan_Default(customerNameIndexRowType, false),
                    coi,
                    customerNameIndexRowType,
                    customerRowType,
                    LookupOption.DISCARD_INPUT),
                removeDescendentTypes(customerRowType));
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            assertEquals(customerRowType, row.rowType());
            assertEquals(expectedRunId++, row.runId());
        }
        assertEquals(2, expectedRunId);
    }

    @Test
    public void testExtractAfterIndexScan()
    {
        Operator plan =
            filter_Default(
                branchLookup_Default(
                    indexScan_Default(customerNameIndexRowType, false),
                    coi,
                    customerNameIndexRowType,
                    customerRowType,
                    LookupOption.DISCARD_INPUT),
                Arrays.asList(orderRowType, itemRowType));
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            // Each customer has 2 orders
            for (int o = 0; o < 2; o++) {
                if (o > 0) {
                    assertTrue((row = cursor.next()) != null);
                }
                assertEquals(orderRowType, row.rowType());
                assertEquals(expectedRunId, row.runId());
                // Each order has 2 items
                for (int i = 0; i < 2; i++) {
                    assertTrue((row = cursor.next()) != null);
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
        Operator plan =
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
                JoinType.LEFT_JOIN);
        RowType coRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            // There should be 2 co rows per run
            for (int co = 0; co < 2; co++) {
                if (co > 0) {
                    assertTrue((row = cursor.next()) != null);
                }
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
        Operator plan =
            project_Default(
                filter_Default(
                    branchLookup_Default(
                        indexScan_Default(customerNameIndexRowType, false),
                        coi,
                        customerNameIndexRowType,
                        customerRowType,
                        LookupOption.DISCARD_INPUT),
                    removeDescendentTypes(customerRowType)),
                customerRowType,
                Arrays.asList(field(customerRowType, 1)));
        RowType customerNameRowType = plan.rowType();
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int expectedRunId = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            // There should be 1 projected row per run
            for (int p = 0; p < 1; p++) {
                if (p > 0) {
                    assertTrue((row = cursor.next()) != null);
                }
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
        Operator plan = groupScan_Default(coi);
        Cursor cursor = cursor(plan, adapter);
        cursor.open(NO_BINDINGS);
        int count = 0;
        RowBase row;
        while ((row = cursor.next()) != null) {
            assertEquals(-1, row.runId());
            count++;
        }
        assertEquals(14, count);
    }

    private Set<AisRowType> removeDescendentTypes(AisRowType type)
    {
        Set<AisRowType> keepTypes = type.schema().userTableTypes();
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }
}
