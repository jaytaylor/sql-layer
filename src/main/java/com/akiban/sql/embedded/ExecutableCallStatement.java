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

import com.akiban.sql.embedded.JDBCParameterMetaData.ParameterType;

import com.akiban.ais.model.Parameter;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ColumnBinding;
import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StaticMethodCallNode;
import com.akiban.sql.server.ServerCallInvocation;

import java.util.ArrayList;
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
        if (invocation != null) {
            JDBCParameterMetaData parameterMetaData = parameterMetaData(invocation, 
                                                                        sqlParams);
            switch (invocation.getCallingConvention()) {
            case LOADABLE_PLAN:
                return ExecutableLoadableOperator.executableStatement(invocation, parameterMetaData, call, context);
            case JAVA:
                return ExecutableJavaMethod.executableStatement(invocation, parameterMetaData, call, context);
            case SCRIPT_FUNCTION_JAVA:
                return ExecutableScriptFunctionJavaRoutine.executableStatement(invocation, parameterMetaData, call, context);
            case SCRIPT_BINDINGS:
                return ExecutableScriptBindingsRoutine.executableStatement(invocation, parameterMetaData, call, context);
            }
        }
        throw new UnsupportedSQLException("Unknown routine", call);
    }

    public static JDBCParameterMetaData parameterMetaData(ServerCallInvocation invocation) {
        List<ParameterType> params = new ArrayList<ParameterType>();
        
        return new JDBCParameterMetaData(params);
    }

    protected static JDBCParameterMetaData parameterMetaData(ServerCallInvocation invocation,
                                                             List<ParameterNode> sqlParams) {
        int nparams = sqlParams.size();
        ParameterType[] ptypes = new ParameterType[nparams];
        for (int i = 0; i < nparams; i++) {
            int usage = invocation.parameterUsage(i);
            if (usage < 0) continue;
            Parameter param = invocation.getRoutineParameter(usage);
            try {
                ptypes[i] = new ParameterType(param.getName(), 
                                              ColumnBinding.getType(param));
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
        }
        return new JDBCParameterMetaData(Arrays.asList(ptypes));
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }
    
}
