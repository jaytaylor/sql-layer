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

package com.akiban.sql.server;

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

public class ServerSessionTracer implements SessionTracer {
    
    // PostgresSessionTracer interface
    
    public ServerSessionTracer(int sessionId,
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
