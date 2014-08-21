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

package com.foundationdb.sql.server;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.TransactionInProgressException;
import com.foundationdb.server.error.TransactionReadOnlyException;
import com.foundationdb.server.service.session.Session;

import com.foundationdb.server.service.transaction.TransactionService;

import java.util.Date;

public class ServerTransaction
{
    public static enum PeriodicallyCommit {
        /** The system commits when you call commit **/
        OFF("false"),
        /** The system commits periodically maintaining **/
        ON("true"),
        /**
         *  The system commits and closes the user-level transaction requiring the client to begin a new transaction.
         *  For jdbc, and probably other drivers, it will create the new transaction automatically for the user.
         */
        USER_LEVEL("userLevel");

        private String propertyName;

        PeriodicallyCommit(String propertyName) {
            this.propertyName = propertyName;
        }

        public static PeriodicallyCommit fromProperty(String name) {
            if (name == null) return OFF;
            for (PeriodicallyCommit opt : values()) {
                if (name.equals(opt.propertyName))
                    return opt;
            }
            throw new InvalidParameterValueException(
                    String.format("Invalid name: %s for TransactionPeriodicallyCommitOption", name));
        }
    }

    private final Session session;
    private final TransactionService txnService;
    private boolean readOnly;
    private PeriodicallyCommit periodicallyCommit;
    private Date transactionTime;
    
    /** Begin a new transaction or signal an exception. */
    public ServerTransaction(ServerSession server, 
                             boolean readOnly, PeriodicallyCommit periodicallyCommit) {
        this.session = server.getSession();
        this.txnService = server.getTransactionService();
        this.readOnly = readOnly;
        this.periodicallyCommit = periodicallyCommit;
        txnService.beginTransaction(session);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public PeriodicallyCommit getPeriodicallyCommit() {
        return periodicallyCommit;
    }

    public void setPeriodicallyCommit(PeriodicallyCommit periodicallyCommit) {
        this.periodicallyCommit = periodicallyCommit;
    }

    public void checkTransactionMode(ServerStatement.TransactionMode transactionMode) {
        switch (transactionMode) {
        case NONE:
        case NEW:
        case NEW_WRITE:
            throw new TransactionInProgressException();
        case WRITE:
            if (readOnly)
                throw new TransactionReadOnlyException();
            beforeUpdate();
        break;
        case IMPLICIT_COMMIT:
            throw new IllegalArgumentException(transactionMode + " must be handled externally");
        }
    }

    public void beforeUpdate() {
    }

    public void afterUpdate() {
        txnService.checkStatementForeignKeys(session);
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

    public boolean shouldPeriodicallyCommit() {
        return txnService.shouldPeriodicallyCommit(session);
    }

    public void checkPeriodicallyCommit() {
        // USER_LEVEL is handled higher up
        if (periodicallyCommit == PeriodicallyCommit.ON) {
            txnService.periodicallyCommit(session);
        }
    }

    /** Return the transaction's time, which is fixed the first time
     * something asks for it. */
    public Date getTime(ServerSession server) {
        if (transactionTime == null)
            transactionTime = server.currentTime();
        return transactionTime;
    }

}
