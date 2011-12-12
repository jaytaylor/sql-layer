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

import com.akiban.server.error.TransactionInProgressException;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.error.TransactionReadOnlyException;
import com.akiban.server.service.session.Session;

import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PostgresTransaction
{
    private Transaction transaction;
    private boolean readOnly;
    private Date transactionTime;
    
    /** Begin a new transaction or signal an exception. */
    public PostgresTransaction(PostgresServerSession server, boolean readOnly) {
        transaction = server.getTreeService().getTransaction(server.getSession());
        boolean transactionBegun = false;
        try {
            transaction.begin();
        }
        catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        } 
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void checkTransactionMode(PostgresStatement.TransactionMode transactionMode) {
        switch (transactionMode) {
        case NONE:
        case NEW:
        case NEW_WRITE:
            throw new TransactionInProgressException();
        case WRITE:
            if (readOnly)
                throw new TransactionReadOnlyException();
        }
    }

    /** Commit transaction. */
    public void commit() {
        try {
            transaction.commit();            
        }
        catch (PersistitException ex) {
            throw new PersistItErrorException(ex);
        }
        finally {
            transaction.end();
        }
    }

    /** Rollback transaction. */
    public void rollback() {
        try {
            transaction.rollback();
        }
        catch (RollbackException ex) {
        }
        catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
        finally {
            transaction.end();
        }
    }

    /** Abort transaction that still exists on exit. */
    public void abort() {
        transaction.end();
    }
    
    /** Return the transaction's time, which is fixed the first time
     * something asks for it. */
    public Date getTime() {
        if (transactionTime == null)
            transactionTime = new Date();
        return transactionTime;
    }

}
