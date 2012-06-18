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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import org.junit.Test;

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
