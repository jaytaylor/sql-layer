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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.akiban.sql.server.ServerSessionTracer;

public class SessionTracerTest {
    
    @Test
    public void testBasicUse() {
        SessionTracer tracer = new ServerSessionTracer(1, false);
        assertEquals(false, tracer.isEnabled());
        tracer.enable();
        assertEquals(true, tracer.isEnabled());
        tracer.beginEvent("firstEvent");
        tracer.endEvent();
        tracer.beginEvent("firstEvent");
        Object[] currentEvents = tracer.getCurrentEvents();
        assertEquals(1, currentEvents.length);
        tracer.endEvent();
        currentEvents = tracer.getCurrentEvents();
        assertEquals(0, currentEvents.length);
        Object[] completedEvents = tracer.getCompletedEvents();
        assertEquals(2, completedEvents.length);
        Event firstEvent = (Event) completedEvents[0];
        Event secondEvent = (Event) completedEvents[1];
        assertEquals(firstEvent, secondEvent);
        assertNotSame(firstEvent.getLastDuration(), secondEvent.getLastDuration());
    }
    
    @Test
    public void testManyEvents() {
        SessionTracer tracer = new ServerSessionTracer(1, true);
        for (int i = 0; i < 1000; i++) {
            tracer.beginEvent("event:" + i);
            tracer.endEvent();
        }
        Event anEvent = tracer.getEvent("event:" + 987);
        assertTrue(anEvent.getLastDuration() > 0);
        Object[] completedEvents = tracer.getCompletedEvents();
        /* we only ever supposed to keep 100 completed events at any time */
        assertEquals(100, completedEvents.length);
    }

}
