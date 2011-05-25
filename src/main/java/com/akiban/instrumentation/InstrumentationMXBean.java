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

import java.util.Date;
import java.util.Set;

public interface InstrumentationMXBean {
    
    Set<Integer> getCurrentSessions();
    
    /*
     * whether instrumentation is enabled for all sessions
     */
    boolean isEnabled();
    void enable();
    void disable();
    
    /*
     * whether instrumentation is enabled for a specific session
     */
    boolean isEnabled(int sessionId);
    void enable(int sessionId);
    void disable(int sessionId);
    
    /*
     * information on individual sessions being traced
     */
    String getSqlText(int sessionId);
    String getRemoteAddress(int sessionId);
    Date getStartTime(int sessionId);
    long getProcessingTime(int sessionId);
    /* below are only for the last statement executed */
    long getParseTime(int sessionId);
    long getOptimizeTime(int sessionId);
    long getExecuteTime(int sessionId);
    int getNumberOfRowsReturned(int sessionId);
    
    Object[] getCurrentEvents(int sessionId);

}