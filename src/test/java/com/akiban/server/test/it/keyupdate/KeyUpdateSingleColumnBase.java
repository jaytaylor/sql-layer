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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Index;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;

import static com.akiban.server.test.it.keyupdate.Schema.*;
import static junit.framework.Assert.*;

public abstract class KeyUpdateSingleColumnBase extends KeyUpdateBase
{
    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdate() throws Exception
    {
        // Set customer.priority = 80 for order 133
        TestRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 80L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.priority = 81 for order 133. Duplicates are fine.
        TestRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 81L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }


    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdate() throws Exception
    {
        // Set customer.when = 9000 for order 133
        TestRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_when, 9000L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.when = 9001 for order 133
        TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        Long oldWhen = (Long) newOrderRow.put(o_when, 9001L);
        assertEquals("old order.when", Long.valueOf(9009L), oldWhen);
        try {
            dbUpdate(oldOrderRow, newOrderRow);

            // Make sure such a row actually exists!
            TestRow shouldHaveConflicted = testStore.find(new HKey(vendorRD, 1L, customerRD, 11L, orderRD, 111L));
            assertNotNull("shouldHaveConflicted not found", shouldHaveConflicted);
            assertEquals(9001L, shouldHaveConflicted.getFields().get(o_when));

            fail("update should have failed with duplicate key");
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        TestRow confirmOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        assertSameFields(oldOrderRow, confirmOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderUpdateIsNoOp() throws Exception
    {
        // Update a row to its same values
        TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }
}
