
package com.akiban.server.service.monitor;

import java.util.List;

public interface SessionMonitor {
    /** The id of the session being monitored. */
    int getSessionId();

    /** The id of the session that called this one or -1 if none. */
    int getCallerSessionId();

    /** What kind of server is this a session for? */
    String getServerType();

    /** The remote IP address of a network connection or <code>null</code>. */
    String getRemoteAddress();

    /** The system time at which this session started. */
    long getStartTimeMillis();
    
    /** The number of queries executed. */
    int getStatementCount();

    /** The SQL of the current / last statement. */
    String getCurrentStatement();    

    /** The time at which the current statement began executing or <code>-1</code>. */
    long getCurrentStatementStartTimeMillis();

    /** The time at which the current statement completed or <code>-1</code>. */
    long getCurrentStatementEndTimeMillis();

    /** The time for which current statement ran. */
    long getCurrentStatementDurationMillis();

    /** The prepared statement name if current statement was prepared. */
    String getCurrentStatementPreparedName();

    /** The number of rows returned / affected by the last statement
     * or <code>-1</code> if unknown, not applicable or in
     * progress. 
     */
    int getRowsProcessed();

    /** The current stage of the session. */
    MonitorStage getCurrentStage();
    
    /** The time in nanoseconds last spent in the given stage. */
    long getLastTimeStageNanos(MonitorStage stage);

    /** The total time in nanoseconds spent in the given stage. */
    long getTotalTimeStageNanos(MonitorStage stage);

    /** Get total time in nanoseconds not spent idle. */
    long getNonIdleTimeNanos();

    /** Get any open cursors. */
    List<CursorMonitor> getCursors();

    /** Get any prepared statements. */
    List<PreparedStatementMonitor> getPreparedStatements();
}
