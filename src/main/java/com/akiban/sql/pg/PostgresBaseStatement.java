
package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.util.tap.InOutTap;

import java.util.List;

/**
 * Common lock and tap handling for executable statements.
 */
public abstract class PostgresBaseStatement implements PostgresStatement
{
    protected long aisGeneration;
    protected abstract InOutTap executeTap();
    protected abstract InOutTap acquireLockTap();

    protected void lock(PostgresQueryContext context, DXLFunction operationType)
    {
        acquireLockTap().in();
        executeTap().in();
        try {
            context.lock(operationType);
        } 
        finally {
            acquireLockTap().out();
        }
    }

    protected void unlock(PostgresQueryContext context, DXLFunction operationType, boolean lockSuccess)
    {
        if (lockSuccess)
            context.unlock(operationType);
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
