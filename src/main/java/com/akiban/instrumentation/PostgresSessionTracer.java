package com.akiban.instrumentation;

import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

public class PostgresSessionTracer implements SessionTracer {
    
    private final static int MAX_EVENTS = 100;

    private int sessionId;
    private int traceLevel;
    private boolean enabled;
    private Map<String, Event> events;
    private Queue<Event> completedEvents;
    private Stack<Event> currentEvents;
    
    public PostgresSessionTracer(int sessionId) {
        this.sessionId = sessionId;
        this.traceLevel = 0;
        this.enabled = false;
        events = new HashMap<String, Event>();
        completedEvents = new LinkedList<Event>();
        currentEvents = new Stack<Event>();
    }
    
    @Override
    public void beginEvent(String eventName) {
        if (enabled) {
            Event ev = events.get(eventName);
            if (ev == null) {
                ev = new EventImpl(eventName, sessionId);
                events.put(eventName, ev);
            }
            ev.start();
            currentEvents.push(ev);
        }
    }

    @Override
    public void endEvent(String eventName) {
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
    
    private void addCompletedEvent(Event ev) {
        completedEvents.add(ev);
        if (completedEvents.size() > MAX_EVENTS) {
            completedEvents.remove();
        }
    }
    
}
