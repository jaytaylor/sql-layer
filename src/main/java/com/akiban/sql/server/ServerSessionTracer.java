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

package com.akiban.sql.pg;

import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import com.akiban.server.service.instrumentation.Event;
import com.akiban.server.service.instrumentation.EventImpl;
import com.akiban.server.service.instrumentation.SessionTracer;

public class PostgresSessionTracer implements SessionTracer {
    
    // PostgresSessionTracer interface
    
    public PostgresSessionTracer(int sessionId,
                                 boolean enabled) {
        this.sessionId = sessionId;
        this.currentStatement = null;
        this.remoteAddress = null;
        this.startTime = System.currentTimeMillis();
        this.nrows = 0;
        this.traceLevel = 0;
        this.enabled = enabled;
        this.events = new HashMap<String, Event>();
        this.completedEvents = new LinkedList<Event>();
        this.currentEvents = new Stack<Event>();
    }
    
    public void setCurrentStatement(String stmt) {
        currentStatement = stmt;
    }
    
    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }
    
    public void setNumberOfRowsReturned(int nrows) {
        this.nrows = nrows;
    }
    
    public int getNumberOfRowsReturned() {
        return nrows;
    }
    
    // SessionTracer interface
    
    @Override
    public void beginEvent(String eventName) {
        if (enabled) {
            Event ev = events.get(eventName);
            if (ev == null) {
                ev = new EventImpl(eventName, sessionId, true);
                events.put(eventName, ev);
            }
            ev.start();
            currentEvents.push(ev);
        }
    }

    @Override
    public void endEvent() {
        if (enabled) {
            try {
                Event ev = currentEvents.pop();
                if (ev == null) {
                    /*
                     * this could happen if instrumentation was enabled during
                     * an event. The same is true for the EmptyStackException
                     * that could be thrown by the call to pop() above.
                     */
                    return;
                }
                ev.stop();
                addCompletedEvent(ev);
            } catch (EmptyStackException e) {
                return;
            }
        }
    }

    @Override
    public Event getEvent(String eventName) {
        return events.get(eventName);
    }
    
    @Override
    public Object[] getCurrentEvents() {
        return currentEvents.toArray();
    }
    
    @Override
    public Object[] getCompletedEvents() {
        return completedEvents.toArray();
    }

    @Override
    public void setTraceLevel(int level) {
        traceLevel = level;
    }

    @Override
    public int getTraceLevel() {
        return traceLevel;
    }

    @Override
    public void enable() {
        /* enable instrumentation for all events */
        for (Event ev : events.values()) {
            ev.enable();
        }
        enabled = true;
    }

    @Override
    public void disable() {
        /* disable instrumentation for all events */
        for (Event ev : events.values()) {
            ev.disable();
        }
        events.clear(); /* should we do this? */
        completedEvents.clear();
        currentEvents.clear();
        enabled = false;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public String getCurrentStatement() {
        return currentStatement;
    }
    
    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public Date getStartTime() {
        return new Date(startTime);
    }
    
    @Override
    public long getProcessingTime() {
        return (System.currentTimeMillis() - startTime);
    }
    
    @Override
    public long getEventTime(String eventName) {
        Event ev = events.get(eventName);
        if (ev != null) {
            return ev.getLastDuration();
        }
        return 0;
    }
    
    @Override
    public long getTotalEventTime(String eventName) {
        Event ev = events.get(eventName);
        if (ev != null) {
            return ev.getTotalTime();
        }
        return 0;
    }
    
    // helper methods
    
    private void addCompletedEvent(Event ev) {
        completedEvents.add(ev);
        if (completedEvents.size() > MAX_EVENTS) {
            completedEvents.remove();
        }
    }
    
    // state
    
    private final static int MAX_EVENTS = 100;

    private int sessionId;
    private String currentStatement;
    private String remoteAddress;
    private long startTime;
    private int nrows;
    private int traceLevel;
    private boolean enabled;
    private Map<String, Event> events;
    private Queue<Event> completedEvents;
    private Stack<Event> currentEvents;
    
}
