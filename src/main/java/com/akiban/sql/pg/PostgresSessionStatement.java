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

import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import java.io.IOException;

/** SQL statements that affect session / environment state. */
public class PostgresSessionStatement implements PostgresStatement
{
    enum Operation {
        USE,
        BEGIN_TRANSACTION, COMMIT_TRANSACTION, ROLLBACK_TRANSACTION
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
    public void execute(PostgresServerSession server, int maxrows)
            throws IOException, StandardException {
        doOperation(server);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
            messenger.writeString(statement.statementToString());
            messenger.sendMessage();
        }
    }

    protected void doOperation(PostgresServerSession server) throws StandardException {
        switch (operation) {
        case USE:
            // TODO: From the appropriate kind of statement, which
            // does not exist in the parser yet, although <CONNECT> is
            // known to be a reserved word.
            server.setDefaultSchemaName("...");
            break;
        case BEGIN_TRANSACTION:
            beginTransaction(server);
            break;
        case COMMIT_TRANSACTION:
            commitTransaction(server);
            break;
        case ROLLBACK_TRANSACTION:
            rollbackTransaction(server);
            break;
        }
    }

    protected void beginTransaction(PostgresServerSession server) 
            throws StandardException {
        Transaction transaction = (Transaction)
            server.getAttribute("transaction");
        if (transaction != null)
            throw new StandardException("A transaction is already in progress.");
        transaction = server.getServiceManager()
            .getTreeService().getTransaction(server.getSession());
        try {
            transaction.begin();
        }
        catch (PersistitException ex) {
            throw new StandardException(ex);
        }
        server.setAttribute("transaction", transaction);
    }

    protected void commitTransaction(PostgresServerSession server) 
            throws StandardException {
        Transaction transaction = (Transaction)
            server.getAttribute("transaction");
        if (transaction == null)
            throw new StandardException("No transaction is in progress.");
        try {
            transaction.commit();
        }
        catch (PersistitException ex) {
            throw new StandardException(ex);
        }
        finally {
            transaction.end();
        }
        server.setAttribute("transaction", null);
    }

    protected void rollbackTransaction(PostgresServerSession server) 
            throws StandardException {
        Transaction transaction = (Transaction)
            server.getAttribute("transaction");
        if (transaction == null)
            throw new StandardException("No transaction is in progress.");
        try {
            transaction.rollback();
        }
        catch (PersistitException ex) {
            throw new StandardException(ex);
        }
        catch (RollbackException ex) {
        }
        finally {
            transaction.end();
        }
        server.setAttribute("transaction", null);
    }

}
