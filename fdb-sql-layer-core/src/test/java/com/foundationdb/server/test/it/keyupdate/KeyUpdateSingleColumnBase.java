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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import org.junit.Test;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.*;

public abstract class KeyUpdateSingleColumnBase extends KeyUpdateBase
{
    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdate() throws Exception
    {
        // Set customer.priority = 80 for order 133
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 80L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.priority = 81 for order 133. Duplicates are fine.
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 81L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }


    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdate() throws Exception
    {
        // Set customer.when = 9000 for order 133
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_when, 9000L, customerRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.when = 9001 for order 133
        KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
        
        Long oldWhen = newOrderRow.value(o_when).getInt64();
        newOrderRow.valueAt(o_when).putInt64(9001L);
        assertEquals("old order.when", Long.valueOf(9009L), oldWhen);
        try {
            dbUpdate(oldOrderRow, newOrderRow);

            // Make sure such a row actually exists!
            KeyUpdateRow shouldHaveConflicted = testStore.find(new HKey(vendorRD, 1L, customerRD, 11L, orderRD, 111L));
            assertNotNull("shouldHaveConflicted not found", shouldHaveConflicted);
            assertEquals(9001L, shouldHaveConflicted.value(o_when).getInt64());

            fail("update should have failed with duplicate key");
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        KeyUpdateRow confirmOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        assertSameFields(oldOrderRow, confirmOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderUpdateIsNoOp() throws Exception
    {
        // Update a row to its same values
        KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, 133L));
        KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }
}
