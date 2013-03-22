
package com.akiban.sql.pg;

import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.TransactionControlNode;

import java.util.List;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresSessionStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        switch (stmt.getNodeType()) {
        case NodeTypes.SET_SCHEMA_NODE:
            return PostgresSessionStatement.Operation.USE.getStatement(stmt);
        case NodeTypes.TRANSACTION_CONTROL_NODE:
            {
                PostgresSessionStatement.Operation operation;
                switch (((TransactionControlNode)stmt).getOperation()) {
                case BEGIN:
                    operation = PostgresSessionStatement.Operation.BEGIN_TRANSACTION;
                    break;
                case COMMIT:
                    operation = PostgresSessionStatement.Operation.COMMIT_TRANSACTION;
                    break;
                case ROLLBACK:
                    operation = PostgresSessionStatement.Operation.ROLLBACK_TRANSACTION;
                    break;
                default:
                    assert false : "Unknown operation " + stmt;
                    operation = null;
                }
                return new PostgresSessionStatement(operation, stmt);
            }
        case NodeTypes.SET_TRANSACTION_ISOLATION_NODE:
            return PostgresSessionStatement.Operation.TRANSACTION_ISOLATION.getStatement(stmt);
        case NodeTypes.SET_TRANSACTION_ACCESS_NODE:
            return PostgresSessionStatement.Operation.TRANSACTION_ACCESS.getStatement(stmt);
        case NodeTypes.SET_CONFIGURATION_NODE:
            return PostgresSessionStatement.Operation.CONFIGURATION.getStatement(stmt);
        default:
            return null;
        }
    }
}
