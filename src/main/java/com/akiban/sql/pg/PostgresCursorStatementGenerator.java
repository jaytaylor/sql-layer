
package com.akiban.sql.pg;

import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;

/** SQL statements related to manipulation of prepared statements and cursors. */
public class PostgresCursorStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresCursorStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        switch (stmt.getNodeType()) {
        case NodeTypes.PREPARE_STATEMENT_NODE:
            return new PostgresPrepareStatement();
        case NodeTypes.DECLARE_STATEMENT_NODE:
            return new PostgresDeclareStatement();
        case NodeTypes.EXECUTE_STATEMENT_NODE:
            return new PostgresExecuteStatement();
        case NodeTypes.FETCH_STATEMENT_NODE:
            return new PostgresFetchStatement();
        case NodeTypes.CLOSE_STATEMENT_NODE:
        case NodeTypes.DEALLOCATE_STATEMENT_NODE:
            return new PostgresCloseCursorStatement();
        default:
            return null;
        }
    }
}
