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

package com.foundationdb.sql.embedded;

import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaMethod;
import com.foundationdb.sql.server.ServerJavaRoutine;

import com.foundationdb.qp.operator.QueryBindings;

import java.lang.reflect.Method;

class ExecutableJavaMethod extends ExecutableJavaRoutine
{
    private Method method;
    
    protected ExecutableJavaMethod(Method method,
                                   ServerCallInvocation invocation,
                                   long aisGeneration,
                                   JDBCParameterMetaData parameterMetaData) {
        super(invocation, aisGeneration, parameterMetaData);
        this.method = method;
    }

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        Method method = conn.getRoutineLoader().loadJavaMethod(conn.getSession(),
                                                               invocation.getRoutineName());
        long aisGeneration = context.getAIS().getGeneration();
        return new ExecutableJavaMethod(method, invocation, aisGeneration, parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context, QueryBindings bindings) {
        return new ServerJavaMethod(context, bindings, invocation, method);
    }
    
}
