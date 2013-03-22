
package com.akiban.sql.pg;

import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.script.ScriptFunctionJavaRoutine;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

import java.util.List;
import java.io.IOException;

public class PostgresScriptFunctionJavaRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptInvoker> pool;

    public static PostgresScriptFunctionJavaRoutine statement(PostgresServerSession server, 
                                                              ServerCallInvocation invocation,
                                                              List<String> columnNames, 
                                                              List<PostgresType> columnTypes,
                                                              PostgresType[] parameterTypes,
                                                              boolean usesPValues) {
        ScriptPool<ScriptInvoker> pool = server.getRoutineLoader()
            .getScriptInvoker(server.getSession(), invocation.getRoutineName());
        return new PostgresScriptFunctionJavaRoutine(pool, invocation,
                                                     columnNames, columnTypes,
                                                     parameterTypes, usesPValues);
    }

    protected PostgresScriptFunctionJavaRoutine(ScriptPool<ScriptInvoker> pool,
                                                ServerCallInvocation invocation,
                                                List<String> columnNames, 
                                                List<PostgresType> columnTypes,
                                                PostgresType[] parameterTypes,
                                                boolean usesPValues) {
        super(invocation, columnNames, columnTypes, parameterTypes, usesPValues);
        this.pool = pool;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context) {
        return new ScriptFunctionJavaRoutine(context, invocation, pool);
    }
    
}
