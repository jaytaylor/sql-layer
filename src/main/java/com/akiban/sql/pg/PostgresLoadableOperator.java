
package com.akiban.sql.pg;

import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerCallInvocation;

import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.io.IOException;
import java.util.List;

public class PostgresLoadableOperator extends PostgresOperatorStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableOperator: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadableOperator: acquire shared lock");

    private ServerCallInvocation invocation;

    protected PostgresLoadableOperator(LoadableOperator loadableOperator, 
                                       ServerCallInvocation invocation,
                                       List<String> columnNames, List<PostgresType> columnTypes, 
                                       PostgresType[] parameterTypes,
                                       boolean usesPValues)
    {
        super(null);
        super.init(loadableOperator.plan(), null, columnNames, columnTypes, parameterTypes, null, usesPValues);
        this.invocation = invocation;
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        context = PostgresLoadablePlan.setParameters(context, invocation, usesPValues());
        ServerCallContextStack.push(context, invocation);
        try {
            return super.execute(context, maxrows);
        }
        finally {
            ServerCallContextStack.pop(context, invocation);
        }
    }

}
