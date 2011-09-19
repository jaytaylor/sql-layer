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
import com.akiban.sql.optimizer.OperatorCompiler_New;
import com.akiban.sql.optimizer.plan.BasePlannable;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

import java.util.List;

/** SQL statement to explain another one. */
public class PostgresExplainStatementGenerator_New extends PostgresBaseStatementGenerator
{
    private OperatorCompiler_New compiler;

    public PostgresExplainStatementGenerator_New(PostgresServerSession server) {
        compiler = (OperatorCompiler_New)server.getAttribute("compiler");
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
        BasePlannable result = compiler.compile((DMLStatementNode)innerStmt, params);
        return new PostgresExplainStatement(result.explainPlan());
    }

}
