
package com.akiban.sql.pg;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.error.UnableToExplainException;
import com.akiban.server.error.UnsupportedExplainException;
import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.rule.ExplainPlanContext;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

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
