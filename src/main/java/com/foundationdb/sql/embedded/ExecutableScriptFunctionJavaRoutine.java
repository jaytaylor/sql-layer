/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.embedded;

import com.akiban.qp.operator.QueryBindings;
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
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context, QueryBindings bindings) {
        return new ScriptFunctionJavaRoutine(context, bindings, invocation, pool);
    }
    
}
