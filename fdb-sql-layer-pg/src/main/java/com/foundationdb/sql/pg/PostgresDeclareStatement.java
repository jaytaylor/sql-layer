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

import com.foundationdb.sql.parser.DeclareStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.qp.operator.QueryBindings;

import java.util.List;
import java.io.IOException;

public class PostgresDeclareStatement extends PostgresBaseCursorStatement
{
    private String name;
    private String sql;
    private StatementNode stmt;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        DeclareStatementNode declare = (DeclareStatementNode)stmt;
        this.name = declare.getName();
        this.stmt = declare.getStatement();
        this.sql = sql.substring(this.stmt.getBeginOffset(), this.stmt.getEndOffset() + 1);
        return this;
    }
    
    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.declareStatement(name, sql, stmt);
        return commandComplete("DECLARE");
    }
    
    @Override
    public boolean putInCache() {
        return false;
    }

}
