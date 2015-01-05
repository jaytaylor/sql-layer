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

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

public abstract class SessionMonitorBase implements SessionMonitor {
    private final int sessionID;
    private final long startTimeMillis;
    private MonitorStage currentStage;
    private long currentStageStartNanos;
    private long[] lastNanos = new long[MonitorStage.values().length];
    private long[] totalNanos = new long[MonitorStage.values().length];
    private String currentStatement, currentStatementPreparedName;
    private long currentStatementStartTime = -1;
    private long currentStatementEndTime = -1;
    private int rowsProcessed = 0;
    private UserMonitor user = null; 
    
    private StatementCounter<StatementTypes, Long> statementCounters = new StatementCounter<StatementTypes, Long>();

    protected SessionMonitorBase(int sessionID) {
        this.sessionID = sessionID;
        this.startTimeMillis = System.currentTimeMillis();
    }


    /* SessionMonitorBase methods */

    public void startStatement(String statement) {
        startStatement(statement, System.currentTimeMillis());
    }

    public void startStatement(String statement, long startTime) {
        startStatement(statement, null, startTime);
    }

    public void startStatement(String statement, String preparedName) {
        startStatement(statement, preparedName, System.currentTimeMillis());
    }

    public void startStatement(String statement, String preparedName, long startTime) {
        statementCounters.countEvent(StatementTypes.STATEMENT);
        currentStatement = statement;
        currentStatementPreparedName = preparedName;
        currentStatementStartTime = startTime;
        currentStatementEndTime = -1;
        rowsProcessed = -1;
    }

    public void endStatement(int rowsProcessed) {
        currentStatementEndTime = System.currentTimeMillis();
        this.rowsProcessed = rowsProcessed;
        if (user != null) {
            user.statementRun();
        }
    }
    
    public void countEvent(StatementTypes type) {
        statementCounters.countEvent(type);
    }
    
    public void failStatement() {
        statementCounters.countEvent(StatementTypes.FAILED);
        endStatement(-1);
    }

    // Caller can sequence all stages and avoid any gaps at the cost of more complicated
    // exception handling, or just enter & leave and accept a tiny bit
    // unaccounted for.
    public void enterStage(MonitorStage stage) {
        long now = System.nanoTime();
        if (currentStage != null) {
            long delta = now - currentStageStartNanos;
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

    @Override
    public int getSessionId() {
        return sessionID;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    @Override
    public long getStatementCount() {
        return statementCounters.getCount(StatementTypes.STATEMENT);
    }
    
    @Override 
    public long getCount(StatementTypes type) {
        return statementCounters.getCount(type);
    }
    
    @Override
    public String getCurrentStatement() {
        return currentStatement;
    }

    @Override
    public String getCurrentStatementPreparedName() {
        return currentStatementPreparedName;
    }

    @Override
    public long getCurrentStatementStartTimeMillis() {
        return currentStatementStartTime;
    }

    @Override
    public long getCurrentStatementEndTimeMillis() {
        return currentStatementEndTime;
    }

    @Override
    public long getCurrentStatementDurationMillis() {
        if (currentStatementEndTime < 0)
            return -1;
        else
            return currentStatementEndTime - currentStatementStartTime;
    }

    @Override
    public int getRowsProcessed() {
        return rowsProcessed;
    }

    @Override
    public MonitorStage getCurrentStage() {
        return currentStage;
    }

    @Override
    public long getLastTimeStageNanos(MonitorStage stage) {
        return lastNanos[stage.ordinal()];
    }

    @Override
    public long getTotalTimeStageNanos(MonitorStage stage) {
        return lastNanos[stage.ordinal()];
    }

    @Override
    public long getNonIdleTimeNanos() {
        long total = 0;
        for (int i = 1; i < totalNanos.length; i++) {
            total += totalNanos[i];
        }
        return total;
    }

    public List<CursorMonitor> getCursors() {
        return Collections.emptyList();
    }

    public List<PreparedStatementMonitor> getPreparedStatements() {
        return Collections.emptyList();
    }
    
    public void setUserMonitor(UserMonitor monitor) {
        this.user = monitor;
    }
    
    public UserMonitor getUserMonitor() {
        return this.user;
    }

    private class StatementCounter<K,V> extends EnumMap<StatementTypes,Long> {

        private static final long serialVersionUID = 1L;

        public StatementCounter() {
            super(StatementTypes.class);
        }

        public final void countEvent(StatementTypes statement)
        {
            if (this.containsKey(statement)) {
                this.put(statement,this.get(statement).longValue() + 1L);
            } else {
                put(statement, 1L);
            }
        }
        
        public final long getCount(StatementTypes type) {
            if (this.containsKey(type)) {
                return this.get(type).longValue();
            } else {
                return 0L;
            }
        }
    }
}
