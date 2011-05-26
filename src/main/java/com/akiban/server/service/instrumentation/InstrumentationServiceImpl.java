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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;

public class InstrumentationServiceImpl implements
    InstrumentationService, JmxManageable, Service<InstrumentationService>, 
    InstrumentationMXBean {
    
    // InstrumentationService interface
    
    public synchronized PostgresSessionTracer createSqlSessionTracer(int sessionId) {
        PostgresSessionTracer ret = new PostgresSessionTracer(sessionId, enabled.get());
        currentSqlSessions.put(sessionId, ret);
        return ret;
    }
    
    public synchronized void removeSqlSessionTracer(int sessionId) {
        currentSqlSessions.remove(sessionId);
    }
    
    public synchronized PostgresSessionTracer getSqlSessionTracer(int sessionId) {
        return currentSqlSessions.get(sessionId);
    }

    // Service interface
    
    @Override
    public InstrumentationService cast() {
        return this;
    }

    @Override
    public Class<InstrumentationService> castClass() {
        return InstrumentationService.class;
    }

    @Override
    public void start() throws Exception {
        // do we need to synchronize?
        currentSqlSessions = new HashMap<Integer, PostgresSessionTracer>();
        enabled = new AtomicBoolean(false);
    }

    @Override
    public void stop() throws Exception {
        // anything to do?
        currentSqlSessions.clear();
        enabled.set(false);
    }

    @Override
    public void crash() throws Exception {
        // anything to do?
    }
    
    // JmxManageable interface

    @Override
    public final JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Instrumentation", this, InstrumentationMXBean.class);
    }
    
    // InstrumentationMXBean

    @Override
    public Set<Integer> getCurrentSessions() {
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
    public long getParseTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getEventTime("sql: parse");
    }

    @Override
    public long getOptimizeTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getEventTime("sql: optimize");
    }

    @Override
    public long getExecuteTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getEventTime("sql: execute");
    }

    @Override
    public long getEventTime(int sessionId, String eventName) {
        return getSqlSessionTracer(sessionId).getEventTime(eventName);
    }

    @Override
    public long getTotalEventTime(int sessionId, String eventName) {
        return getSqlSessionTracer(sessionId).getTotalEventTime(eventName);
    }

    @Override
    public int getNumberOfRowsReturned(int sessionId) {
        return getSqlSessionTracer(sessionId).getNumberOfRowsReturned();
    }
    
    // state
    
    private Map<Integer, PostgresSessionTracer> currentSqlSessions;
    
    private AtomicBoolean enabled;

}
