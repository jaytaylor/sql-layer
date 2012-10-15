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

package com.akiban.sql.pg;

import java.util.Date;
import java.util.Set;

public interface PostgresMXBean {
        
    /**
     * @param sessionId connection ID
     * @return text of current/last query this connection executed
     */
    String getSqlString(int sessionId);
    
    /**
     * @param sessionId connection ID
     * @return client's IP address
     */
    String getRemoteAddress(int sessionId);

    int getStatementCacheCapacity();
    void setStatementCacheCapacity(int capacity);
    int getStatementCacheHits();
    int getStatementCacheMisses();
    void resetStatementCache();
    
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

    /** Cancel any running query for the given connection. */
    void cancelQuery(int sessionId);
    /** Kill the given connection. */
    void killConnection(int sessionId);
}
