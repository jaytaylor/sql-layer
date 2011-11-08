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
import java.util.Set;

public interface PostgresMXBean {
        
    /**
     * @param pid connection ID
     * @return text of current/last query this connection executed
     */
    String getSqlString(int pid);
    
    /**
     * @param pid connection ID
     * @return client's IP address
     */
    String getRemoteAddress(int pid);

    int getStatementCacheCapacity();
    void setStatementCacheCapacity(int capacity);
    int getStatementCacheHits();
    int getStatementCacheMisses();
    
    Set<Integer> getCurrentSessions();
    
    /*
     * whether instrumentation is enabled for all sessions
     */
    boolean isInstrumentationEnabled();
    void enableInstrumentation();
    void disableInstrumentation();
    
    /*
     * whether instrumentation is enabled for a specific session
     */
    boolean isInstrumentationEnabled(int sessionId);
    void enableInstrumentation(int sessionId);
    void disableInstrumentation(int sessionId);
    
    /*
     * information on individual sessions being traced
     */
    Date getStartTime(int sessionId);
    long getProcessingTime(int sessionId);
    /* below are only for the last statement executed */
    long getEventTime(int sessionId, String eventName);
    long getTotalEventTime(int sessionId, String eventName);

    /*
     * Returns the uptime of the Postgres Server in nanoseconds.
     */
    long getUptime();

    /*
     * Loadable plans
     */
    void clearPlans();
    String loadPlan(String jarFilePath, String className);
}
