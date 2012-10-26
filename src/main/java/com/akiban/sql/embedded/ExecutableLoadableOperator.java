/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.List;

class ExecutableLoadableOperator extends ExecutableQueryOperatorStatement
{
    private ServerRoutineInvocation invocation;

    public static ExecutableStatement executableStatement(ServerRoutineInvocation invocation,
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
                                         ServerRoutineInvocation invocation,
                                         JDBCResultSetMetaData resultSetMetaData,
                                         JDBCParameterMetaData parameterMetaData) {
        super(loadableOperator.plan(), resultSetMetaData, parameterMetaData);
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
        List<ResultColumn> columns = new ArrayList<ResultColumn>(jdbcTypes.length);
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

    protected static EmbeddedQueryContext setParameters(EmbeddedQueryContext context, ServerRoutineInvocation invocation) {
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
