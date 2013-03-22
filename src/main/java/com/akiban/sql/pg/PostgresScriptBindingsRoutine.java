
package com.akiban.sql.pg;

import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.script.ScriptBindingsRoutine;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

import java.util.List;
import java.io.IOException;

public class PostgresScriptBindingsRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    public static PostgresScriptBindingsRoutine statement(PostgresServerSession server, 
                                                          ServerCallInvocation invocation,
                                                          List<String> columnNames, 
                                                          List<PostgresType> columnTypes,
                                                          PostgresType[] parameterTypes,
                                                          boolean usesPValues) {
        ScriptPool<ScriptEvaluator> pool = server.getRoutineLoader()
            .getScriptEvaluator(server.getSession(), invocation.getRoutineName());
        return new PostgresScriptBindingsRoutine(pool, invocation,
                                                 columnNames, columnTypes,
                                                 parameterTypes, usesPValues);
    }

    protected PostgresScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
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
        return new ScriptBindingsRoutine(context, invocation, pool);
    }
    
}
