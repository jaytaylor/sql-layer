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
import com.akiban.server.error.UnsupportedConfigurationException;
import com.akiban.server.error.UnsupportedParametersException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.aisddl.SchemaDDL;
import com.akiban.sql.parser.AccessMode;
import com.akiban.sql.parser.IsolationLevel;
import com.akiban.sql.parser.SetConfigurationNode;
import com.akiban.sql.parser.SetSchemaNode;
import com.akiban.sql.parser.SetTransactionAccessNode;
import com.akiban.sql.parser.SetTransactionIsolationNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.StatementType;

import java.util.Arrays;
import java.io.IOException;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatement implements PostgresStatement
{
    enum Operation {
        USE, CONFIGURATION,
        BEGIN_TRANSACTION, COMMIT_TRANSACTION, ROLLBACK_TRANSACTION,
        TRANSACTION_ISOLATION, TRANSACTION_ACCESS
    };

    public static final String[] ALLOWED_CONFIGURATION = new String[] {
      "client_encoding", "DateStyle", "geqo", "ksqo",
      "zeroDateTimeBehavior"
    };

    private Operation operation;
    private StatementNode statement;
    
    public PostgresSessionStatement(Operation operation, StatementNode statement) {
        this.operation = operation;
        this.statement = statement;
    }

    @Override
    public PostgresStatement getBoundStatement(Object[] parameters,
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
            {
                SetSchemaNode node = (SetSchemaNode)statement;
                String schemaName = (node.statementType() == StatementType.SET_SCHEMA_USER ? 
                                     server.getProperty("user") : node.getSchemaName());
                if (SchemaDDL.checkSchema(server.getAIS(), schemaName)) {
                    server.setDefaultSchemaName(schemaName);
                } 
                else {
                    throw new NoSuchSchemaException(schemaName);
                }
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
        case TRANSACTION_ACCESS:
            {
                SetTransactionAccessNode node = (SetTransactionAccessNode)statement;
                boolean current = node.isCurrent();
                boolean readOnly = (node.getAccessMode() == 
                                    AccessMode.READ_ONLY_ACCESS_MODE);
                if (current)
                    server.setTransactionReadOnly(readOnly);
                else
                    server.setTransactionDefaultReadOnly(readOnly);
            }
            break;
        case CONFIGURATION:
            {
                SetConfigurationNode node = (SetConfigurationNode)statement;
                String variable = node.getVariable();
                if (!Arrays.asList(ALLOWED_CONFIGURATION).contains(variable))
                    throw new UnsupportedConfigurationException(variable);
                server.setProperty(variable, node.getValue());
            }
            break;
        default:
            throw new UnsupportedSQLException("session control", statement);
        }
    }

}
