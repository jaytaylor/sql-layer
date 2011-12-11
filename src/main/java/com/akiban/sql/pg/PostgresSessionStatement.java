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

import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.error.UnsupportedParametersException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.aisddl.SchemaDDL;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.SetSchemaNode;
import com.akiban.sql.parser.StatementType;

import java.io.IOException;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatement implements PostgresStatement
{
    enum Operation {
        USE,
        BEGIN_TRANSACTION, COMMIT_TRANSACTION, ROLLBACK_TRANSACTION,
        TRANSACTION_ISOLATION, TRANSACTION_ACCESS
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
                                               boolean defaultColumnBinary)  {
        if (parameters != null)
            throw new UnsupportedParametersException();
        return this;
    }

    @Override
    public void sendDescription(PostgresServerSession server, boolean always) 
            throws IOException {
        if (always) {
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public int execute(PostgresServerSession server, int maxrows)
            throws IOException {
        doOperation(server);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(statement.statementToString());
            messenger.sendMessage();
        }
        return 0;
    }

    protected void doOperation(PostgresServerSession server) {
        switch (operation) {
        case USE:
            final SetSchemaNode node = (SetSchemaNode)statement;
            
            final String schemaName = (node.statementType() == StatementType.SET_SCHEMA_USER ? 
                    server.getProperty("user") : node.getSchemaName());
            if (SchemaDDL.checkSchema(server.getAIS(), schemaName)) {
                server.setDefaultSchemaName(schemaName);
            } 
            else {
                throw new NoSuchSchemaException(schemaName);
            }
            break;
        case BEGIN_TRANSACTION:
            server.beginTransaction();
            break;
        case COMMIT_TRANSACTION:
            server.commitTransaction();
            break;
        case ROLLBACK_TRANSACTION:
            server.rollbackTransaction();
            break;
        default:
            throw new UnsupportedSQLException("session control", statement);
        }
    }

}
