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

package com.akiban.sql.server;

import com.akiban.server.service.monitor.MonitorStage;
import com.akiban.server.service.monitor.SessionMonitor;

public class ServerSessionMonitor implements SessionMonitor {
    private final String serverType;
    private final int sessionId;
    private final long startTime;
    private int callerSessionId = -1;
    private String remoteAddress;
    private int statementCount;
    private String currentStatement;
    private long currentStatementStartTime = -1;
    private long currentStatementEndTime = -1;
    private int rowsProcessed;
    private long[] lastNanos, totalNanos;
    private MonitorStage currentStage;
    private long currentStageStartNanos;

    public ServerSessionMonitor(String serverType, int sessionId) {
        this.serverType = serverType;
        this.sessionId = sessionId;
        startTime = System.currentTimeMillis();
        lastNanos = new long[MonitorStage.values().length];
        totalNanos = new long[MonitorStage.values().length];
    }
    
    public void setCallerSessionId(int callerSessionId) {
        this.callerSessionId = callerSessionId;
    }

    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }
    
    public void startStatement(String statement) {
        startStatement(statement, System.currentTimeMillis());
    }

    public void startStatement(String statement, long startTime) {
        if (statement != null) {  // TODO: Remove when always passed by PG server.
            statementCount++;
            currentStatement = statement;
        }
        currentStatementStartTime = startTime;
        currentStatementEndTime = -1;
        rowsProcessed = -1;
    }

    public void endStatement(int rowsProcessed) {
        currentStatementEndTime = System.currentTimeMillis();
        this.rowsProcessed = rowsProcessed;
    }

    public long getCurrentStatementDurationMillis() {
        if (currentStatementEndTime < 0)
            return -1;
        else
            return currentStatementEndTime - currentStatementStartTime;
    }

    // Caller can sequence all stages and avoid any gaps at the cost of more complicated
    // exception handling, or just enter & leave and accept a tiny bit
    // unaccounted for.
    public void enterStage(MonitorStage stage) {
        long now = System.nanoTime();
        if (currentStage != null) {
            long delta = currentStageStartNanos - now;
            lastNanos[currentStage.ordinal()] = delta;
            totalNanos[currentStage.ordinal()] += delta;
        }
        currentStage = stage;
        currentStageStartNanos = now;
    }

    public void leaveStage() {
        enterStage(null);
    }

    /* SessionMonitor */

    public int getSessionId() {
        return sessionId;
    }

    public int getCallerSessionId() {
        return callerSessionId;
    }

    public String getServerType() {
        return serverType;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public long getStartTimeMillis() {
        return startTime;
    }
    
    public int getStatementCount() {
        return statementCount;
    }

    public String getCurrentStatement() {
        return currentStatement;
    }

    public long getCurrentStatementStartTimeMillis() {
        return currentStatementStartTime;
    }

    public long getCurrentStatementEndTimeMillis() {
        return currentStatementEndTime;
    }

    public int getRowsProcessed() {
        return rowsProcessed;
    }

    public MonitorStage getCurrentStage() {
        return currentStage;
    }
    
    public long getLastTimeStageNanos(MonitorStage stage) {
        return lastNanos[stage.ordinal()];
    }

    public long getTotalTimeStageNanos(MonitorStage stage) {
        return totalNanos[stage.ordinal()];
    }

    public long getNonIdleTimeNanos() {
        long total = 0;
        for (int i = 1; i < totalNanos.length; i++) {
            total += totalNanos[i];
        }
        return total;
    }

}
