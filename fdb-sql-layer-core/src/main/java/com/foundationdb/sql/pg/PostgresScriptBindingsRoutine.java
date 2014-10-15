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
import com.foundationdb.server.service.routines.ScriptEvaluator;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.sql.script.ScriptBindingsRoutine;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaRoutine;

import java.util.List;

public class PostgresScriptBindingsRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    public static PostgresScriptBindingsRoutine statement(PostgresServerSession server, 
                                                          ServerCallInvocation invocation,
                                                          List<String> columnNames, 
                                                          List<PostgresType> columnTypes,
                                                          List<Column> aisColumns,
                                                          PostgresType[] parameterTypes) {
        ScriptPool<ScriptEvaluator> pool = server.getRoutineLoader()
            .getScriptEvaluator(server.getSession(), invocation.getRoutineName());
        return new PostgresScriptBindingsRoutine(pool, invocation,
                                                 columnNames, columnTypes, aisColumns,
                                                 parameterTypes);
    }

    protected PostgresScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
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
        return new ScriptBindingsRoutine(context, bindings, invocation, pool);
    }
    
}
