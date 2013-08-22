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

import com.foundationdb.sql.parser.CloseStatementNode;
import com.foundationdb.sql.parser.DeallocateStatementNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.qp.operator.QueryBindings;

import java.util.List;
import java.io.IOException;

public class PostgresCloseCursorStatement extends PostgresBaseCursorStatement
{
    private String name;
    private boolean preparedStatement;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        if (stmt.getNodeType() == NodeTypes.DEALLOCATE_STATEMENT_NODE) {
            name = ((DeallocateStatementNode)stmt).getName();
            preparedStatement = true;
        }
        else {
            name = ((CloseStatementNode)stmt).getName();
            preparedStatement = false;
        }
        return this;
    }
    
    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        if (preparedStatement)
            server.deallocatePreparedStatement(name);
        else
            server.closeBoundPortal(name);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(preparedStatement ? "DEALLOCATE" : "CLOSE");
            messenger.sendMessage();
        }
        return 0;
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
