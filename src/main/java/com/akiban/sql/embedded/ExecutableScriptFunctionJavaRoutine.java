
package com.akiban.sql.embedded;

import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.script.ScriptFunctionJavaRoutine;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

class ExecutableScriptFunctionJavaRoutine extends ExecutableJavaRoutine
{
    private ScriptPool<ScriptInvoker> pool;
    
    protected ExecutableScriptFunctionJavaRoutine(ScriptPool<ScriptInvoker> pool,
                                                  ServerCallInvocation invocation,
                                                  JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
        this.pool = pool;
    }

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        ScriptPool<ScriptInvoker> pool = conn.getRoutineLoader().getScriptInvoker(conn.getSession(),
                                                                                  invocation.getRoutineName());
        return new ExecutableScriptFunctionJavaRoutine(pool, invocation, parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context) {
        return new ScriptFunctionJavaRoutine(context, invocation, pool);
    }
    
}
