/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.server.error.UnableToExplainException;
import com.foundationdb.server.error.UnsupportedExplainException;
import com.foundationdb.sql.optimizer.OperatorCompiler;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.rule.ExplainPlanContext;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ExplainStatementNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ParameterNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SQL statement to explain another one. */
public class PostgresExplainStatementGenerator extends PostgresBaseStatementGenerator
{
    private OperatorCompiler compiler;

    public PostgresExplainStatementGenerator(PostgresServerSession server) {
        compiler = (OperatorCompiler)server.getAttribute("compiler");
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        if (stmt.getNodeType() != NodeTypes.EXPLAIN_STATEMENT_NODE)
            return null;
        StatementNode innerStmt = ((ExplainStatementNode)stmt).getStatement();
        if (compiler == null)
            throw new UnsupportedExplainException();
        if (!(innerStmt instanceof DMLStatementNode))
            throw new UnableToExplainException ();
        return new PostgresExplainStatement(compiler);
    }
}
