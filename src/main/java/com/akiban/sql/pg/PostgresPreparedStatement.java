
package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;
import com.akiban.server.service.monitor.PreparedStatementMonitor;

public class PostgresPreparedStatement implements PreparedStatementMonitor
{
    private PostgresServerSession session;
    private String name;
    private String sql;
    private PostgresStatement statement;
    private long prepareTime;

    public PostgresPreparedStatement(PostgresServerSession session, String name,
                                     String sql, PostgresStatement statement,
                                     long prepareTime) {
        this.session = session;
        this.name = name;
        this.sql = sql;
        this.statement = statement;
        this.prepareTime = prepareTime;
    }

    @Override
    public int getSessionId() {
        return session.getSessionMonitor().getSessionId();
    }

    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public String getSQL() {
        return sql;
    }
    
    @Override
    public long getPrepareTimeMillis() {
        return prepareTime;
    }

    @Override
    public int getEstimatedRowCount() {
        CostEstimate costEstimate = statement.getCostEstimate();
        if (costEstimate == null)
            return -1;
        else
            return (int)costEstimate.getRowCount();
    }

    public PostgresStatement getStatement() {
        return statement;
    }

}
