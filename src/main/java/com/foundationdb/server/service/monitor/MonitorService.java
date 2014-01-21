/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.service.monitor;

import com.foundationdb.server.service.session.Session;

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
    void registerSessionMonitor(SessionMonitor sessionMonitor, Session session);

    /** Deregister the given session monitor. */
    void deregisterSessionMonitor(SessionMonitor sessionMonitor, Session session);

    /** Get the session monitor for the given session id. */
    SessionMonitor getSessionMonitor(int sessionId);

    /** Get the session monitor for the given session. */
    SessionMonitor getSessionMonitor(Session session);

    /** Get all registered session monitors. */
    Collection<SessionMonitor> getSessionMonitors();

    /** Log the given SQL to the query log. */
    void logQuery(int sessionId, String sqlText, long duration, int rowsProcessed);

    /** Log last statement from given monitor. */
    void logQuery(SessionMonitor sessionMonitor);
    
    /** Register the given User monitor. */
    void registerUserMonitor (UserMonitor userMonitor);
    
    /** Deregister the given user montitor. */
    void deregisterUserMonitor (UserMonitor userMonitor);
    
    /** Deregister the montor for the given user */
    void deregisterUserMonitor (String userName);
    
    /** Get the user monitor for the given user name. */
    UserMonitor getUserMonitor(String userName);
    
    /** Get the user monitor for the session user */
    UserMonitor getUserMonitor(Session session);
    
    /** Get all the user monitors. */
    Collection<UserMonitor> getUserMonitors();

    //
    // Query Log Control
    //

    /** Is query logging turned on? */
    boolean isQueryLogEnabled();

    /** Turn query log on or off. */
    void setQueryLogEnabled(boolean enabled);

    /** Set the filename for the query log. */
    void setQueryLogFileName(String fileName);

    /** Get the current query log or an empty string if unset. */
    String getQueryLogFileName();

    /** Set minimum number of milliseconds for a query to be logged or {@code -1} if no limit. */
    void setQueryLogThresholdMillis(long threshold);

    /** Get minimum number of milliseconds for a query to be logged or {@code -1} if no limit. */
    long getQueryLogThresholdMillis();
}
