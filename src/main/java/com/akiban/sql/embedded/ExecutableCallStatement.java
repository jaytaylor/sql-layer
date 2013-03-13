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
import com.akiban.ais.model.TableName;
import com.akiban.server.error.UnsupportedSQLException;
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
        JDBCParameterMetaData parameterMetaData = parameterMetaData(invocation, nparams);
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            return ExecutableLoadableOperator.executableStatement(invocation, parameterMetaData, call, context);
        case JAVA:
            return ExecutableJavaMethod.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return ExecutableScriptFunctionJavaRoutine.executableStatement(invocation, parameterMetaData, context);
        case SCRIPT_BINDINGS:
            return ExecutableScriptBindingsRoutine.executableStatement(invocation, parameterMetaData, context);
        default:
            throw new UnsupportedSQLException("Unknown routine", call);
        }
    }

    protected static JDBCParameterMetaData parameterMetaData(ServerCallInvocation invocation,
                                                             int nparams) {
        ParameterType[] ptypes = new ParameterType[nparams];
        for (int i = 0; i < nparams; i++) {
            int usage = invocation.parameterUsage(i);
            if (usage < 0) continue;
            ptypes[i] = new ParameterType(invocation.getRoutineParameter(usage));
        }
        return new JDBCParameterMetaData(Arrays.asList(ptypes));
    }

    public ServerCallInvocation getInvocation() {
        return invocation;
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }
    
}
