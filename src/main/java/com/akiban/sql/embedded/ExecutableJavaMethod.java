
package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

import java.lang.reflect.Method;

class ExecutableJavaMethod extends ExecutableJavaRoutine
{
    private Method method;
    
    protected ExecutableJavaMethod(Method method,
                                   ServerCallInvocation invocation,
                                   JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
        this.method = method;
    }

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        Method method = conn.getRoutineLoader().loadJavaMethod(conn.getSession(),
                                                               invocation.getRoutineName());
        return new ExecutableJavaMethod(method, invocation, parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context) {
        return new ServerJavaMethod(context, invocation, method);
    }
    
}
