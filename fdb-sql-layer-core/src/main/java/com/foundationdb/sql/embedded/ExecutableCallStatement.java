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

import com.foundationdb.sql.embedded.JDBCParameterMetaData.ParameterType;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StaticMethodCallNode;
import com.foundationdb.sql.server.ServerCallInvocation;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.Arrays;
import java.util.List;

abstract class ExecutableCallStatement extends ExecutableStatement
{
    protected ServerCallInvocation invocation;
    protected JDBCParameterMetaData parameterMetaData;

    protected ExecutableCallStatement(ServerCallInvocation invocation,
                                      JDBCParameterMetaData parameterMetaData) {
        this.invocation = invocation;
        this.parameterMetaData = parameterMetaData;
    }

    public static ExecutableStatement executableStatement(CallStatementNode call,
                                                          List<ParameterNode> sqlParams,
                                                          EmbeddedQueryContext context) {
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        ServerCallInvocation invocation =
            ServerCallInvocation.of(context.getServer(), methodCall);
        return executableStatement(invocation, call, sqlParams, context);
    }

    public static ExecutableStatement executableStatement(TableName routineName,
                                                          EmbeddedQueryContext context) {
        ServerCallInvocation invocation =
            ServerCallInvocation.of(context.getServer(), routineName);
        return executableStatement(invocation, null, null, context);
    }

    protected static ExecutableStatement executableStatement(ServerCallInvocation invocation,
                                                             CallStatementNode call,
                                                             List<ParameterNode> sqlParams,
                                                             EmbeddedQueryContext context) {
        int nparams = (sqlParams == null) ? invocation.size() : sqlParams.size();
        JDBCParameterMetaData parameterMetaData = parameterMetaData(invocation, nparams, context);
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            return ExecutableLoadableOperator.executableStatement(invocation, parameterMetaData, call, context);
        case JAVA:
            return ExecutableJavaMethod.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return ExecutableScriptFunctionJavaRoutine.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
            return ExecutableScriptBindingsRoutine.executableStatement(invocation, parameterMetaData, context);
        default:
            throw new UnsupportedSQLException("Unknown routine", call);
        }
    }

    protected static JDBCParameterMetaData parameterMetaData(ServerCallInvocation invocation,
                                                             int nparams,
                                                             EmbeddedQueryContext context) {
        ParameterType[] ptypes = new ParameterType[nparams];
        for (int i = 0; i < nparams; i++) {
            int usage = invocation.parameterUsage(i);
            if (usage < 0) continue;
            Parameter parameter = invocation.getRoutineParameter(usage);
            TInstance type = parameter.getType();
            int jdbcType = type.typeClass().jdbcType();
            DataTypeDescriptor sqlType = type.dataTypeDescriptor();
            ptypes[i] = new ParameterType(parameter, sqlType, jdbcType, type);
        }
        return new JDBCParameterMetaData(context.getTypesTranslator(), Arrays.asList(ptypes));
    }

    public ServerCallInvocation getInvocation() {
        return invocation;
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }
    
    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.CALL_STMT;
    }
}
