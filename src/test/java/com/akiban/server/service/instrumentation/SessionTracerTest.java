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
