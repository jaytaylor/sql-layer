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

package com.akiban.server.service.transaction;

import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;

public interface TransactionService extends Service {
    interface Callback {
        void run(Session session);
    }

    /** Returns true if there is a transaction active for the given Session */
    boolean isTransactionActive(Session session);

    /** Returns true if there is a transaction active for the given Session */
    boolean isRollbackPending(Session session);

    /** Begin a new transaction. */
    void beginTransaction(Session session);

    /** Commit the open transaction. */
    void commitTransaction(Session session);

    /** Rollback an open transaction. */
    void rollbackTransaction(Session session);

    /**
     * @return current step for the open transaction.
     */
    int getTransactionStep(Session session);

    /**
     * Sets the current step for the open transaction.
     * @return previous step value.
     */
    int setTransactionStep(Session session, int newStep);

    /**
     * Increments the current step for the open transaction.
     * @return previous step value.
     */
    int incrementTransactionStep(Session session);

    /** Add callback to be invoked when the transactions ends, independent of commit/rollback or success/failure */
    void addEndCallback(Session session, Callback callback);

    /** Add a callback to be invoked upon successful completion of commit. */
    void addCommitCallback(Session session, Callback callback);

    /** Add a callback to be invoked upon successful completion of commit. */
    void addRollbackCallback(Session session, Callback callback);
}
