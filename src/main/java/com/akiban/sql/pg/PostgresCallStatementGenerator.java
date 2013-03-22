
package com.akiban.sql.pg;

import com.akiban.sql.server.ServerCallInvocation;

import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.StaticMethodCallNode;

import com.akiban.server.explain.Explainable;

import java.util.List;

/**
 * SQL statements that affect session / environment state.
 */
public class PostgresCallStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresCallStatementGenerator(PostgresServerSession server)
    {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)
    {
        if (!(stmt instanceof CallStatementNode))
            return null;
        CallStatementNode call = (CallStatementNode)stmt;
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        // This will signal error if undefined, so any special handling of
        // non-AIS CALL statements needs to be tested by an earlier generator.
        ServerCallInvocation invocation = ServerCallInvocation.of(server, methodCall);
        final PostgresStatement pstmt;
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            pstmt = PostgresLoadablePlan.statement(server, invocation);
            break;
        default:
            pstmt = PostgresJavaRoutine.statement(server, invocation,
                                                  params, paramTypes);
        }
        // The above makes extensive use of the AIS. This doesn't fit well into the
        // create and then init, so just mark with AIS now.
        pstmt.setAISGeneration(server.getAIS().getGeneration());
        return pstmt;
    }

    public static Explainable explainable(PostgresServerSession server,
                                          CallStatementNode call, 
                                          List<ParameterNode> params, int[] paramTypes) {
        StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
        ServerCallInvocation invocation = ServerCallInvocation.of(server, methodCall);
        switch (invocation.getCallingConvention()) {
        case LOADABLE_PLAN:
            return PostgresLoadablePlan.explainable(server, invocation);
        default:
            return PostgresJavaRoutine.explainable(server, invocation,
                                                   params, paramTypes);
        }
    }
}
