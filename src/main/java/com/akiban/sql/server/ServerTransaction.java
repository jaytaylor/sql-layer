/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.server;

import com.akiban.server.error.TransactionInProgressException;
import com.akiban.server.error.TransactionReadOnlyException;
import com.akiban.server.service.session.Session;

import com.akiban.server.service.transaction.TransactionService;

import java.util.Date;

public class ServerTransaction
{
    private final Session session;
    private final TransactionService txnService;
    private boolean readOnly;
    private Date transactionTime;
    
    /** Begin a new transaction or signal an exception. */
    public ServerTransaction(ServerSession server, boolean readOnly) {
        this.session = server.getSession();
        this.txnService = server.getTransactionService();
        this.readOnly = readOnly;
        txnService.beginTransaction(session);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void checkTransactionMode(ServerStatement.TransactionMode transactionMode) {
        switch (transactionMode) {
        case NONE:
        case NEW:
        case NEW_WRITE:
            throw new TransactionInProgressException();
        case WRITE:
        case WRITE_STEP_ISOLATED:
            if (readOnly)
                throw new TransactionReadOnlyException();
            beforeUpdate(transactionMode == ServerStatement.TransactionMode.WRITE_STEP_ISOLATED);
        }
    }

    public void beforeUpdate(boolean withStepIsolation) {
        if (withStepIsolation && (txnService.getTransactionStep(session) == 0))
            // On the first non-read statement in a transaction, move
            // to step 1 to enable isolation against later steps.
            // Step 1 will do the update and then we'll move to step 2
            // to make it visible.
            txnService.incrementTransactionStep(session);
    }

    public void afterUpdate(boolean withStepIsolation) {
        if (withStepIsolation && !txnService.isRollbackPending(session))
            txnService.incrementTransactionStep(session);
    }

    /** Commit transaction. */
    public void commit() {
        txnService.commitTransaction(session);
    }

    /** Rollback transaction. */
    public void rollback() {
        txnService.rollbackTransaction(session);
    }

    /** Abort transaction that still exists on exit. */
    public void abort() {
        txnService.rollbackTransactionIfOpen(session);
    }
    
    public boolean isRollbackPending() {
        return txnService.isRollbackPending(session);
    }

    /** Return the transaction's time, which is fixed the first time
     * something asks for it. */
    public Date getTime(ServerSession server) {
        if (transactionTime == null)
            transactionTime = server.currentTime();
        return transactionTime;
    }

}
