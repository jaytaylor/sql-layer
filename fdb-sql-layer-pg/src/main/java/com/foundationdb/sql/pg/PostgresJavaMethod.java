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
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.server.ServerJavaMethod;
import com.foundationdb.sql.server.ServerJavaRoutine;

import java.lang.reflect.Method;
import java.util.List;

public class PostgresJavaMethod extends PostgresJavaRoutine
{
    private Method method;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerCallInvocation invocation,
                                              List<String> columnNames, 
                                              List<PostgresType> columnTypes,
                                              List<Column> aisColumns,
                                              PostgresType[] parameterTypes) {
        Method method = server.getRoutineLoader()
            .loadJavaMethod(server.getSession(), invocation.getRoutineName());
        return new PostgresJavaMethod(method, invocation,
                                      columnNames, columnTypes, aisColumns,
                                      parameterTypes);
    }


    public PostgresJavaMethod() {
    }

    protected PostgresJavaMethod(Method method,
                                 ServerCallInvocation invocation,
                                 List<String> columnNames, 
                                 List<PostgresType> columnTypes,
                                 List<Column> aisColumns,
                                 PostgresType[] parameterTypes) {
        super(invocation, columnNames, columnTypes, aisColumns, parameterTypes);
        this.method = method;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context, QueryBindings bindings) {
        return new ServerJavaMethod(context, bindings, invocation, method);
    }
    
}
