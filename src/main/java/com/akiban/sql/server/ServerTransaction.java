/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
        if (txnService.isTransactionActive(session))
            txnService.rollbackTransaction(session);
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
