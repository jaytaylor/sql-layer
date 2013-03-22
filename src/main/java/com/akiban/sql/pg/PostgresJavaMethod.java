
package com.akiban.sql.pg;

import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

import java.lang.reflect.Method;
import java.util.List;
import java.io.IOException;

public class PostgresJavaMethod extends PostgresJavaRoutine
{
    private Method method;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerCallInvocation invocation,
                                              List<String> columnNames, 
                                              List<PostgresType> columnTypes,
                                              PostgresType[] parameterTypes,
                                              boolean usesPValues) {
        Method method = server.getRoutineLoader()
            .loadJavaMethod(server.getSession(), invocation.getRoutineName());
        return new PostgresJavaMethod(method, invocation,
                                      columnNames, columnTypes,
                                      parameterTypes, usesPValues);
    }


    public PostgresJavaMethod() {
    }

    protected PostgresJavaMethod(Method method,
                                 ServerCallInvocation invocation,
                                 List<String> columnNames, 
                                 List<PostgresType> columnTypes,
                                 PostgresType[] parameterTypes,
                                 boolean usesPValues) {
        super(invocation, columnNames, columnTypes, parameterTypes, usesPValues);
        this.method = method;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context) {
        return new ServerJavaMethod(context, invocation, method);
    }
    
}
