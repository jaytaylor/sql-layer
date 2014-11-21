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

import com.foundationdb.sql.parser.CopyStatementNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

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
                return new PostgresCopyInStatement(compiler.getSchema());
            }
        }
        return null;
    }
}
