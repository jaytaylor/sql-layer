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

import com.akiban.server.error.UnableToExplainException;
import com.akiban.server.error.UnsupportedExplainException;
import com.akiban.sql.optimizer.OperatorCompiler;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

import java.util.List;

/** SQL statement to explain another one. */
public class PostgresExplainStatementGenerator extends PostgresBaseStatementGenerator
{
    private OperatorCompiler compiler;

    public PostgresExplainStatementGenerator(PostgresServerSession server) {
        compiler = (OperatorCompiler)server.getAttribute("compiler");
    }

    @Override
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt, 
                                      List<ParameterNode> params,
                                      int[] paramTypes)  {
        if (stmt.getNodeType() != NodeTypes.EXPLAIN_STATEMENT_NODE)
            return null;
        StatementNode innerStmt = ((ExplainStatementNode)stmt).getStatement();
        if (compiler == null)
            throw new UnsupportedExplainException();
        if (!(innerStmt instanceof DMLStatementNode))
            throw new UnableToExplainException ();
        OperatorCompiler.Result result = compiler.compile(server.getSessionTracer(),
                                                          (DMLStatementNode)innerStmt,
                                                          params);
        return new PostgresExplainStatement(result.explainPlan());
    }

}
