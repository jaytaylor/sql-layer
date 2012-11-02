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

    /** The SQL of the current last statement. */
    String getCurrentStatement();    

    /** The time at which the current statement began executing or <code>-1</code>. */
    long getCurrentStatementStartTimeMillis();

    /** The time at which the current statement completed or <code>-1</code>. */
    long getCurrentStatementEndTimeMillis();

    /** The time for which current statement ran. */
    long getCurrentStatementDurationMillis();

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
}
