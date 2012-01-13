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
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt, 
                                      List<ParameterNode> params, int[] paramTypes)  {
        switch (stmt.getNodeType()) {
        case NodeTypes.SET_SCHEMA_NODE:
            return new PostgresSessionStatement(PostgresSessionStatement.Operation.USE, stmt);
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
            return new PostgresSessionStatement(PostgresSessionStatement.Operation.TRANSACTION_ISOLATION, stmt);
        case NodeTypes.SET_TRANSACTION_ACCESS_NODE:
            return new PostgresSessionStatement(PostgresSessionStatement.Operation.TRANSACTION_ACCESS, stmt);
        case NodeTypes.SET_CONFIGURATION_NODE:
            return new PostgresSessionStatement(PostgresSessionStatement.Operation.CONFIGURATION, stmt);
        default:
            return null;
        }
    }
}
