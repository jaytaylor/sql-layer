package com.akiban.instrumentation;

import java.util.Date;
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
        SessionTracer ret = new PostgresSessionTracer(sessionId, enabled.get());
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
    
    @Override
    public synchronized Set<Integer> getCurrentSessions() {
        return new HashSet<Integer>(currentSqlSessions.keySet());
    }
    
    @Override
    public boolean isEnabled() {
        return enabled.get();
    }    
    
    @Override
    public void enable() {
        for (SessionTracer tracer : currentSqlSessions.values()) {
            tracer.enable();
        }
        enabled.set(true);
    }
    
    @Override
    public void disable() {
        for (SessionTracer tracer : currentSqlSessions.values()) {
            tracer.disable();
        }
        enabled.set(false);
    }

    @Override
    public boolean isEnabled(int sessionId) {
        return getSqlSessionTracer(sessionId).isEnabled();
    }

    @Override
    public void enable(int sessionId) {
        getSqlSessionTracer(sessionId).enable();
    }
    
    @Override
    public void disable(int sessionId) {
        getSqlSessionTracer(sessionId).disable();
    }

    @Override
    public String getSqlText(int sessionId) {
        return getSqlSessionTracer(sessionId).getCurrentStatement();
    }
    
    @Override
    public String getRemoteAddress(int sessionId) {
        return getSqlSessionTracer(sessionId).getRemoteAddress();
    }
    
    @Override
    public Date getStartTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getStartTime();
    }
    
    @Override
    public long getProcessingTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getProcessingTime();
    }
    
    @Override
    public Object[] getCurrentEvents(int sessionId) {
        return getSqlSessionTracer(sessionId).getCurrentEvents();
    }
    
    // state
    
    private static InstrumentationLibrary lib = null;
    
    private Map<Integer, SessionTracer> currentSqlSessions;
    
    private AtomicBoolean enabled;

}
