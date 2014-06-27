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

import com.foundationdb.server.error.MissingDDLParametersException;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.NodeTypes;

import java.util.List;

/** DDL statements executed against AIS. */
public class PostgresDDLStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresDDLStatementGenerator(PostgresServerSession server, PostgresOperatorCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params,
                                          int[] paramTypes) {
        if (!(stmt instanceof DDLStatementNode))
            return null;
        PostgresOperatorStatement opstmt = null;
        if(stmt.getNodeType() == NodeTypes.CREATE_TABLE_NODE && ((CreateTableNode)stmt).getQueryExpression() != null){
            opstmt = new PostgresOperatorStatement(compiler);
        }
        if ((params != null) && !params.isEmpty())
            throw new MissingDDLParametersException ();
        return new PostgresDDLStatement((DDLStatementNode)stmt, sql, opstmt);
    }

    PostgresOperatorCompiler compiler;
}
