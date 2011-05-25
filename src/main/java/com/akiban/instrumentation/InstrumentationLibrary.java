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

package com.akiban.instrumentation;

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.akiban.util.TapMXBeanImpl;

public class InstrumentationLibrary 
    implements InstrumentationMXBean {
    
    // InstrumentationLibrary interface
    
    private InstrumentationLibrary() {
        this.currentSqlSessions = new HashMap<Integer, PostgresSessionTracer>();
        this.enabled = new AtomicBoolean(false);
    }
    
    public static synchronized InstrumentationLibrary initialize() {
        if (lib == null) {
            lib = new InstrumentationLibrary();
        }
        return lib;
    }
    
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
    public long getParseTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getParseTime();
    }
    
    @Override
    public long getOptimizeTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getOptimizeTime();
    }
    
    @Override
    public long getExecuteTime(int sessionId) {
        return getSqlSessionTracer(sessionId).getExecuteTime();
    }
    
    @Override
    public int getNumberOfRowsReturned(int sessionId) {
        return getSqlSessionTracer(sessionId).getNumberOfRowsReturned();
    }
    
    /*@Override
    public Object[] getCurrentEvents(int sessionId) {
        return getSqlSessionTracer(sessionId).getCurrentEvents();
    }*/
    
    /**
     * Register an MXBean to make methods of this class available remotely from
     * JConsole or other JMX client. Does nothing if there already is a
     * registered MXBean.
     * 
     * @throws Exception
     */
    public synchronized void registerMXBean() throws Exception {
        if (!registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Instrumentation");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, mxbeanName);
            registered = true;
        }
    }

    /**
     * Unregister the MXBean created by {@link #registerMXBean()}. Does nothing
     * if there is no registered MXBean.
     * 
     * @throws Exception
     */
    public synchronized void unregisterMXBean() throws Exception {
        if (registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Instrumentation");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(mxbeanName);
            registered = false;
        }
    }
    
    // state
    
    private static InstrumentationLibrary lib = null;
    
    private Map<Integer, PostgresSessionTracer> currentSqlSessions;
    
    private AtomicBoolean enabled;
    
    private boolean registered = false;

}
