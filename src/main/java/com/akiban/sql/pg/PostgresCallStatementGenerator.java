/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.server.error.StalePlanException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.parser.*;

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
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt,
                                      List<ParameterNode> params, int[] paramTypes)
    {
        PostgresLoadablePlan statement = null;
        if (stmt instanceof CallStatementNode) {
            CallStatementNode call = (CallStatementNode)stmt;
            StaticMethodCallNode methodCall = (StaticMethodCallNode)call.methodCall().getJavaValueNode();
            String planName = methodCall.getMethodName();
            Object[] args = null;
            JavaValueNode[] margs = methodCall.getMethodParameters();
            if (margs != null) {
                args = new Object[margs.length];
                for (int i = 0; i < margs.length; i++) {
                    JavaValueNode marg = margs[i];
                    if (marg instanceof SQLToJavaValueNode) {
                        ValueNode sqlArg = ((SQLToJavaValueNode)marg).getSQLValueNode();
                        if (sqlArg instanceof ConstantNode) {
                            args[i] = ((ConstantNode)sqlArg).getValue();
                            continue;
                        }
                    }
                    throw new UnsupportedSQLException("CALL parameter must be constant",
                                                      marg);
                }
            }
            statement = PostgresLoadablePlan.statement(server, planName, args);
            if (statement == null) {
                throw new StalePlanException(planName);
            }
        }
        return statement;
    }
}
