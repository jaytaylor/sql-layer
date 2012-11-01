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

import java.util.Date;

public interface SessionMonitor {
    /** The id of the session being monitored. */
    public int getSessionId();

    /** The id of the session that called this one or -1 if none. */
    public int getCallerSessionId();

    /** What kind of server is this a session for? */
    public String getServerType();

    /** The remote IP address of a network connection or <code>null</code>. */
    public String getRemoteAddress();

    /** The time at which this session started. */
    public Date getStartTime();
    
    /** The number of queries executed. */
    public int getStatementCount();

    /** The SQL of the current last statement. */
    public String getCurrentStatement();    

    /** The time at which the current statement began executing. */
    public Date getCurrentStatementStartTime();

    /** The time at which the current statement completed. */
    public Date getCurrentStatementEndTime();

    /** The number of rows returned / affected by the last statement
     * or <code>-1</code> if unknown, not applicable or in
     * progress. 
     */
    public int getRowsProcessed();

    /** The current stage of the session. */
    public MonitorStage getCurrentStage();
    
    /** The time in nanoseconds last spent in the given stage. */
    public long getLastTimeStageNanos(MonitorStage stage);

    /** The total time in nanoseconds spent in the given stage. */
    public long getTotalTimeStageNanos(MonitorStage stage);
}
