package com.akiban.server.service.instrumentation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SessionTracerTest {
    
    @Test
    public void testBasicUse() {
        SessionTracer tracer = new PostgresSessionTracer(1);
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
        SessionTracer tracer = new PostgresSessionTracer(1, true);
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
