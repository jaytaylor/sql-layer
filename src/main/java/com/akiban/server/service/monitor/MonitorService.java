/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
