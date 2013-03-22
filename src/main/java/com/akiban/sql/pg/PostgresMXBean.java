
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
     * information on individual sessions being monitored
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
