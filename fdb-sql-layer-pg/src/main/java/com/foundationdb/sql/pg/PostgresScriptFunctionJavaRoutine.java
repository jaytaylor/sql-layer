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

package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.routines.ScriptInvoker;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.sql.script.ScriptFunctionJavaRoutine;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaRoutine;

import java.util.List;

public class PostgresScriptFunctionJavaRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptInvoker> pool;

    public static PostgresScriptFunctionJavaRoutine statement(PostgresServerSession server, 
                                                              ServerCallInvocation invocation,
                                                              List<String> columnNames, 
                                                              List<PostgresType> columnTypes,
                                                              List<Column> aisColumns,
                                                              PostgresType[] parameterTypes) {
        long[] aisGeneration = new long[1];
        ScriptPool<ScriptInvoker> pool = server.getRoutineLoader()
            .getScriptInvoker(server.getSession(), invocation.getRoutineName(),
                              aisGeneration);
        return new PostgresScriptFunctionJavaRoutine(pool, invocation,
                                                     columnNames, columnTypes, aisColumns,
                                                     parameterTypes);
    }

    protected PostgresScriptFunctionJavaRoutine(ScriptPool<ScriptInvoker> pool,
                                                ServerCallInvocation invocation,
                                                List<String> columnNames, 
                                                List<PostgresType> columnTypes,
                                                List<Column> aisColumns,
                                                PostgresType[] parameterTypes) {
        super(invocation, columnNames, columnTypes, aisColumns, parameterTypes);
        this.pool = pool;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context, QueryBindings bindings) {
        return new ScriptFunctionJavaRoutine(context, bindings, invocation, pool);
    }
    
}
