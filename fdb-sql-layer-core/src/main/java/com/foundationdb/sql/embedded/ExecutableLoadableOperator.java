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

import com.foundationdb.sql.embedded.JDBCResultSetMetaData.ResultColumn;

import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.SparseArrayQueryBindings;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.List;

class ExecutableLoadableOperator extends ExecutableQueryOperatorStatement
{
    private ServerCallInvocation invocation;

    public static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          CallStatementNode call,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        LoadablePlan<?> plan = 
            conn.getRoutineLoader().loadLoadablePlan(conn.getSession(),
                                                     invocation.getRoutineName());
        JDBCResultSetMetaData resultSetMetaData = resultSetMetaData(plan, context);
        if (plan instanceof LoadableOperator)
            return new ExecutableLoadableOperator((LoadableOperator)plan, invocation,
                                                  resultSetMetaData, parameterMetaData);
        throw new UnsupportedSQLException("Unsupported loadable plan", call);
    }

    protected ExecutableLoadableOperator(LoadableOperator loadableOperator, 
                                         ServerCallInvocation invocation,
                                         JDBCResultSetMetaData resultSetMetaData,
                                         JDBCParameterMetaData parameterMetaData) {
        super(loadableOperator.schema(), loadableOperator.plan(), resultSetMetaData, parameterMetaData, null);
        this.invocation = invocation;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        bindings = setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean success = false;
        stack.push(context, invocation);
        try {
            ExecuteResults results = super.execute(context, bindings);
            success = true;
            return results;
        }
        finally {
            stack.pop(context, invocation, success);
        }
    }

    protected static JDBCResultSetMetaData resultSetMetaData(LoadablePlan<?> plan,
                                                             EmbeddedQueryContext context) {
        List<String> columnNames = plan.columnNames();
        int[] jdbcTypes = plan.jdbcTypes();
        List<ResultColumn> columns = new ArrayList<>(jdbcTypes.length);
        for (int i = 0; i < jdbcTypes.length; i++) {
            String name = columnNames.get(i);
            int jdbcType = jdbcTypes[i];
            DataTypeDescriptor sqlType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            TInstance type = context.getTypesTranslator().typeForSQLType(sqlType);
            ResultColumn column = new ResultColumn(name, jdbcType, sqlType,
                                                   null, type, null);
            columns.add(column);
        }
        return new JDBCResultSetMetaData(context.getTypesTranslator(), columns);
    }

    protected static QueryBindings setParameters(QueryBindings bindings, ServerCallInvocation invocation) {
        if (!invocation.parametersInOrder()) {
            if (invocation.hasParameters()) {
                QueryBindings calleeBindings = new SparseArrayQueryBindings();
                invocation.copyParameters(bindings, calleeBindings);
                bindings = calleeBindings;
            }
            else {
                invocation.copyParameters(null, bindings);
            }
        }
        return bindings;
    }

}
