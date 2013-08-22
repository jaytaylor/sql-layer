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
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

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
