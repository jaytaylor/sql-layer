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

import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.TransactionControlNode;

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
            return PostgresSessionStatement.Operation.SET_CONFIGURATION.getStatement(stmt);
        case NodeTypes.SHOW_CONFIGURATION_NODE:
            return PostgresSessionStatement.Operation.SHOW_CONFIGURATION.getStatement(stmt);
        case NodeTypes.SET_CONSTRAINTS_NODE:
            return PostgresSessionStatement.Operation.SET_CONSTRAINTS.getStatement(stmt);
        default:
            return null;
        }
    }
}
