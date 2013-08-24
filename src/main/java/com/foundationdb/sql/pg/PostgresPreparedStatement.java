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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.server.service.monitor.PreparedStatementMonitor;

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
