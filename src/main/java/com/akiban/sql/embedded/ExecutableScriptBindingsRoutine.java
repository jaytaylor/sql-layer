
package com.akiban.sql.embedded;

import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.script.ScriptBindingsRoutine;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

class ExecutableScriptBindingsRoutine extends ExecutableJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    protected ExecutableScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
                                              ServerCallInvocation invocation,
                                              JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
        this.pool = pool;
    }

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        ScriptPool<ScriptEvaluator> pool = conn.getRoutineLoader().getScriptEvaluator(conn.getSession(),
                                                                                      invocation.getRoutineName());
        return new ExecutableScriptBindingsRoutine(pool, invocation, parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context) {
        return new ScriptBindingsRoutine(context, invocation, pool);
    }
    
}
