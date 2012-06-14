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

package com.akiban.server.service.instrumentation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class EventTest {
    
    @Test
    public void testSimpleUse() {
        /* construct event that is disabled by default */
        Event first = new EventImpl("test", 1, false);
        assertEquals(false, first.isEnabled());
        assertEquals(0, first.getLastDuration());
        first.start();
        first.stop();
        /* event should not be timed if not enabled */
        assertEquals(0, first.getLastDuration());
        first.enable();
        assertEquals(true, first.isEnabled());
        first.start();
        first.stop();
        assertTrue(first.getLastDuration() >= 0);
        first.reset();
        assertEquals(0, first.getLastDuration());
        /* construct event that is enabled by default */
        Event second = new EventImpl("test", 2, true);
        assertEquals(true, second.isEnabled());
        assertEquals(0, second.getTotalTime());
        second.start();
        first.start();
        first.stop();
        second.stop();
        long firstTime = first.getTotalTime();
        long secondTime = second.getTotalTime();
        assertTrue(String.format("firstTime: %s", firstTime), firstTime >= 0);
        assertTrue(secondTime >= 0);
        assertTrue(String.format("firstTime: %s, secondTime: %s", firstTime, secondTime), secondTime >= firstTime);
    }
    
    @Test
    public void testEmptyName() {
        Event first = new EventImpl(null, 1, true);
        first.start();
        first.stop();
        assertTrue(first.getTotalTime() >= 0);
    }

}
