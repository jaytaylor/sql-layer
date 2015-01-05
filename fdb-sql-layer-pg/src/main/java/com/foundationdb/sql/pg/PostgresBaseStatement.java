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
import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.util.tap.InOutTap;

import java.util.List;

/**
 * Common lock and tap handling for executable statements.
 */
public abstract class PostgresBaseStatement implements PostgresStatement
{
    protected long aisGeneration;
    protected abstract InOutTap executeTap();

    protected void preExecute(PostgresQueryContext context, DXLFunction operationType)
    {
        executeTap().in();
    }

    protected void postExecute(PostgresQueryContext context, DXLFunction operationType)
    {
        executeTap().out();
    }

    @Override
    public boolean hasAISGeneration() {
        return aisGeneration != 0;
    }

    @Override
    public void setAISGeneration(long generation) {
        aisGeneration = generation;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        return this;
    }

    @Override
    public boolean putInCache() {
        return true;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return null;
    }

}
