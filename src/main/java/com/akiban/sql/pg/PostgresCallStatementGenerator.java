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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerRoutineInvocation;

import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.StaticMethodCallNode;
import com.akiban.sql.server.ServerStatement;

import java.util.List;

/**
 * SQL statements that affect session / environment state.
 */
public class PostgresCallStatementGenerator extends PostgresBaseStatementGenerator
{
    private static final PostgresStubStatement CALL_STATEMENT_STUB = new PostgresStubStatement(
            ServerStatement.TransactionMode.WRITE_STEP_ISOLATED,
            ServerStatement.TransactionAbortedMode.NOT_ALLOWED
    );

    public PostgresCallStatementGenerator(PostgresServerSession server)
    {
    }

    @Override
    public PostgresStatement generateInitial(PostgresServerSession server,
                                             StatementNode stmt,
                                             List<ParameterNode> params, int[] paramTypes)
    {
        if (stmt instanceof CallStatementNode)
            return CALL_STATEMENT_STUB;
        return null;
    }

    @Override
    public PostgresStatement generateFinal(PostgresServerSession server, PostgresStatement pstmt, StatementNode stmt,
                                           List<ParameterNode> params, int[] paramTypes) {
        if (pstmt != CALL_STATEMENT_STUB) {
            CallStatementNode call = (CallStatementNode)stmt;
            StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
            ServerRoutineInvocation invocation =
                ServerRoutineInvocation.of(server, methodCall);
            if (invocation != null) {
                switch (invocation.getCallingConvention()) {
                case LOADABLE_PLAN:
                    return PostgresLoadablePlan.statement(server, invocation, 
                                                          paramTypes);
                default:
                    return PostgresJavaMethod.statement(server, invocation, 
                                                        params, paramTypes);
                }
            }
        }
        return null;
    }
}
