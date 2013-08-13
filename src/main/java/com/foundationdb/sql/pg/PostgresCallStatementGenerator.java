/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerCallInvocation;

import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.StaticMethodCallNode;

import com.akiban.server.error.CantCallScriptLibraryException;
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
        case SCRIPT_LIBRARY:
            throw new CantCallScriptLibraryException(stmt);
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
