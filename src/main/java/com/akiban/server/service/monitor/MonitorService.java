/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.service.monitor;

import java.util.Collection;
import java.util.Map;

public interface MonitorService {
    /** Register the given server monitor. */
    void registerServerMonitor(ServerMonitor serverMonitor);

    /** Deregister the given server monitor. */
    void deregisterServerMonitor(ServerMonitor serverMonitor);

    /** Get all registered server monitors. */
    Map<String,ServerMonitor> getServerMonitors();

    /** Allocate a unique id for a new session. */
    int allocateSessionId();

    /** Register the given session monitor. */
    void registerSessionMonitor(SessionMonitor sessionMonitor);

    /** Deregister the given session monitor. */
    void deregisterSessionMonitor(SessionMonitor sessionMonitor);

    /** Get the session monitor for the given session id. */
    SessionMonitor getSessionMonitor(int sessionId);

    /** Get all registered session monitors. */
    Collection<SessionMonitor> getSessionMonitors();
    
    /** Is query logging turned on? */
    boolean isQueryLogEnabled();

    /** Log the given SQL to the query log. */
    void logQuery(int sessionId, String sqlText, long duration, int rowsProcessed);

    /** Log last statement from given monitor. */
    void logQuery(SessionMonitor sessionMonitor);
}
