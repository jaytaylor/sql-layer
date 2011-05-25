package com.akiban.instrumentation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class InstrumentationLibrary implements InstrumentationMXBean {
    
    // InstrumentationLibrary interface
    
    private InstrumentationLibrary() {
        this.currentSqlSessions = new HashMap<Integer, SessionTracer>();
        this.enabled = new AtomicBoolean(false);
    }
    
    public static synchronized InstrumentationLibrary initialize() {
        if (lib == null) {
            lib = new InstrumentationLibrary();
        }
        return lib;
    }
    
    public synchronized SessionTracer createSqlSessionTracer(int sessionId) {
        SessionTracer ret = new PostgresSessionTracer(sessionId);
        currentSqlSessions.put(sessionId, ret);
        return ret;
    }
    
    public synchronized void removeSqlSessionTracer(int sessionId) {
        currentSqlSessions.remove(sessionId);
    }
    
    public synchronized SessionTracer getSqlSessionTracer(int sessionId) {
        return currentSqlSessions.get(sessionId);
    }
    
    // InstrumentationMXBean
    
    public synchronized Set<Integer> getCurrentConnections() {
        return new HashSet<Integer>(currentSqlSessions.keySet());
    }
    
    public boolean isEnabled() {
        return enabled.get();
    }    
    
    public void enable() {
        for (SessionTracer tracer : currentSqlSessions.values()) {
            tracer.enable();
        }
        enabled.set(true);
    }
    
    public void disable() {
        for (SessionTracer tracer : currentSqlSessions.values()) {
            tracer.disable();
        }
        enabled.set(false);
    }

    public boolean isEnabled(int sessionId) {
        return getSqlSessionTracer(sessionId).isEnabled();
    }

    public void enable(int sessionId) {
        getSqlSessionTracer(sessionId).enable();
    }
    
    public void disable(int sessionId) {
        getSqlSessionTracer(sessionId).disable();
    }
    
    public Object[] getCurrentEvents(int sessionId) {
        return getSqlSessionTracer(sessionId).getCurrentEvents();
    }
    
    // state
    
    private static InstrumentationLibrary lib = null;
    
    private Map<Integer, SessionTracer> currentSqlSessions;
    
    private AtomicBoolean enabled;

}
