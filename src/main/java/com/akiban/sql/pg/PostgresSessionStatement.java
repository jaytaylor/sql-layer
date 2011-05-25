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

import com.akiban.sql.parser.StatementNode;

import com.akiban.sql.StandardException;

import java.io.IOException;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatement implements PostgresStatement
{
    enum Operation {
        USE
    };

    private Operation operation;
    private StatementNode statement;
    
    public PostgresSessionStatement(Operation operation, StatementNode statement) {
        this.operation = operation;
        this.statement = statement;
    }

    @Override
    public PostgresStatement getBoundStatement(String[] parameters,
                                               boolean[] columnBinary, 
                                               boolean defaultColumnBinary) 
            throws StandardException {
        if (parameters != null)
            throw new StandardException("Parameters not supported.");
        return this;
    }

    @Override
    public void sendDescription(PostgresServerSession server, boolean always) 
            throws IOException, StandardException {
        if (always) {
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessenger.NO_DATA_TYPE);
            messenger.sendMessage();
        }
    }

    @Override
    public int execute(PostgresServerSession server, int maxrows)
            throws IOException, StandardException {
        switch (operation) {
        case USE:
            // TODO: From the appropriate kind of statement, which
            // does not exist in the parser yet, although <CONNECT> is
            // known to be a reserved word.
            server.setDefaultSchemaName("...");
            break;
        }
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
            messenger.writeString(statement.statementToString());
            messenger.sendMessage();
        }
        return 0;
    }

}
