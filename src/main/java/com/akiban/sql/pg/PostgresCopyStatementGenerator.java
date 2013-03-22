
package com.akiban.sql.pg;

import com.akiban.sql.parser.CopyStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;

/** COPY statement. */
public class PostgresCopyStatementGenerator extends PostgresBaseStatementGenerator
{
    private PostgresOperatorCompiler compiler;

    public PostgresCopyStatementGenerator(PostgresServerSession server) {
        compiler = (PostgresOperatorCompiler)server.getAttribute("compiler");
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        if (stmt.getNodeType() == NodeTypes.COPY_STATEMENT_NODE) {
            switch (((CopyStatementNode)stmt).getMode()) {
            case FROM_TABLE:
            case FROM_SUBQUERY:
                return new PostgresCopyOutStatement(compiler);
            case TO_TABLE:
                return new PostgresCopyInStatement();
            }
        }
        return null;
    }
}
