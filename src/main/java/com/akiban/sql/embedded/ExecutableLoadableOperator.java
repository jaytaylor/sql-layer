
package com.akiban.sql.embedded;

import com.akiban.sql.embedded.JDBCResultSetMetaData.ResultColumn;

import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.types.DataTypeDescriptor;

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
        JDBCResultSetMetaData resultSetMetaData = resultSetMetaData(plan);
        if (plan instanceof LoadableOperator)
            return new ExecutableLoadableOperator((LoadableOperator)plan, invocation,
                                                  resultSetMetaData, parameterMetaData);
        throw new UnsupportedSQLException("Unsupported loadable plan", call);
    }

    protected ExecutableLoadableOperator(LoadableOperator loadableOperator, 
                                         ServerCallInvocation invocation,
                                         JDBCResultSetMetaData resultSetMetaData,
                                         JDBCParameterMetaData parameterMetaData) {
        super(loadableOperator.plan(), resultSetMetaData, parameterMetaData, null);
        this.invocation = invocation;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        context = setParameters(context, invocation);
        ServerCallContextStack.push(context, invocation);
        try {
            return super.execute(context);
        }
        finally {
            ServerCallContextStack.pop(context, invocation);
        }
    }

    protected static JDBCResultSetMetaData resultSetMetaData(LoadablePlan<?> plan) {
        List<String> columnNames = plan.columnNames();
        int[] jdbcTypes = plan.jdbcTypes();
        List<ResultColumn> columns = new ArrayList<>(jdbcTypes.length);
        for (int i = 0; i < jdbcTypes.length; i++) {
            String name = columnNames.get(i);
            int jdbcType = jdbcTypes[i];
            DataTypeDescriptor sqlType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            TInstance tInstance = TypesTranslation.toTInstance(sqlType);
            ResultColumn column = new ResultColumn(name, jdbcType, sqlType,
                                                   null, tInstance, null);
            columns.add(column);
        }
        return new JDBCResultSetMetaData(columns);
    }

    protected static EmbeddedQueryContext setParameters(EmbeddedQueryContext context, ServerCallInvocation invocation) {
        if (!invocation.parametersInOrder()) {
            if (invocation.hasParameters()) {
                EmbeddedQueryContext calleeContext = 
                    new EmbeddedQueryContext(context.getServer());
                invocation.copyParameters(context, calleeContext, Types3Switch.ON);
                context = calleeContext;
            }
            else {
                invocation.copyParameters(null, context, Types3Switch.ON);
            }
        }
        return context;
    }

}
