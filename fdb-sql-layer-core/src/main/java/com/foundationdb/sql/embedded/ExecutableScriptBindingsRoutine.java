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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.routines.ScriptEvaluator;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.sql.script.ScriptBindingsRoutine;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaMethod;
import com.foundationdb.sql.server.ServerJavaRoutine;

class ExecutableScriptBindingsRoutine extends ExecutableJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    protected ExecutableScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
                                              ServerCallInvocation invocation,
                                              long aisGeneration,
                                              JDBCParameterMetaData parameterMetaData) {
        super(invocation, aisGeneration, parameterMetaData);
        this.pool = pool;
    }

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        long[] aisGeneration = new long[1];
        ScriptPool<ScriptEvaluator> pool = conn.getRoutineLoader().getScriptEvaluator(conn.getSession(),
                                                                                      invocation.getRoutineName(),
                                                                                      aisGeneration);
        return new ExecutableScriptBindingsRoutine(pool, invocation, aisGeneration[0], parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context, QueryBindings bindings) {
        return new ScriptBindingsRoutine(context, bindings, invocation, pool);
    }
    
}
