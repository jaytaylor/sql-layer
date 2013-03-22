
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
