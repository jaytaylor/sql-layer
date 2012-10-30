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

package com.akiban.server.store;

import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.util.MultipleCauseException;
import com.google.inject.Inject;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import static com.akiban.server.service.session.Session.Key;
import static com.akiban.server.service.session.Session.StackKey;

public class PersistitTransactionService implements TransactionService {
    private static final Key<Transaction> TXN_KEY = Key.named("TXN_KEY");
    private static final StackKey<Callback> PRE_COMMIT_KEY = StackKey.stackNamed("TXN_PRE_COMMIT");
    private static final StackKey<Callback> AFTER_END_KEY = StackKey.stackNamed("TXN_AFTER_END");
    private static final StackKey<Callback> AFTER_COMMIT_KEY = StackKey.stackNamed("TXN_AFTER_COMMIT");
    private static final StackKey<Callback> AFTER_ROLLBACK_KEY = StackKey.stackNamed("TXN_AFTER_ROLLBACK");

    private final TreeService treeService;

    @Inject
    public PersistitTransactionService(TreeService treeService) {
        this.treeService = treeService;
    }

    @Override
    public boolean isTransactionActive(Session session) {
        Transaction txn = getTransaction(session);
        return (txn != null) && txn.isActive();
    }

    @Override
    public boolean isRollbackPending(Session session) {
        Transaction txn = getTransaction(session);
        return (txn != null) && txn.isRollbackPending();
    }

    @Override
    public long getTransactionStartTimestamp(Session session) {
        Transaction txn = getAndSetTransaction(session);
        requireActive(txn);
        return txn.getStartTimestamp();
    }

    @Override
    public void beginTransaction(Session session) {
        Transaction txn = getAndSetTransaction(session);
        requireInactive(txn); // Do not want to use Persistit nesting
        try {
            txn.begin();
        } catch(PersistitException e) {
            PersistitAdapter.handlePersistitException(session, e);
        }
    }

    @Override
    public void commitTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        RuntimeException re = null;
        try {
            runCallbacks(session, PRE_COMMIT_KEY, txn.getStartTimestamp(), null);
            txn.commit();
            runCallbacks(session, AFTER_COMMIT_KEY, txn.getCommitTimestamp(), null);
        } catch(RuntimeException e) {
            re = e;
        } catch(PersistitException e) {
            re = PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            end(session, txn, re);
        }
    }

    @Override
    public void rollbackTransaction(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        RuntimeException re = null;
        try {
            txn.rollback();
            runCallbacks(session, AFTER_ROLLBACK_KEY, -1, null);
        } catch(RuntimeException e) {
            re = e;
        } finally {
            end(session, txn, re);
        }
    }

    @Override
    public void rollbackTransactionIfOpen(Session session) {
        Transaction txn = getTransaction(session);
        if((txn != null) && txn.isActive()) {
            rollbackTransaction(session);
        }
    }

    @Override
    public int getTransactionStep(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        return txn.getStep();
    }

    @Override
    public int setTransactionStep(Session session, int newStep) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        return txn.setStep(newStep);
    }

    @Override
    public int incrementTransactionStep(Session session) {
        Transaction txn = getTransaction(session);
        requireActive(txn);
        return txn.incrementStep();
    }

    @Override
    public void addPreCommitCallback(Session session, Callback callback) {
        requireActive(getTransaction(session));
        session.push(PRE_COMMIT_KEY, callback);
    }

    @Override
    public void addEndCallback(Session session, Callback callback) {
        requireActive(getTransaction(session));
        session.push(AFTER_END_KEY, callback);
    }

    @Override
    public void addCommitCallback(Session session, Callback callback) {
        requireActive(getTransaction(session));
        session.push(AFTER_COMMIT_KEY, callback);
    }

    @Override
    public void addRollbackCallback(Session session, Callback callback) {
        requireActive(getTransaction(session));
        session.push(AFTER_ROLLBACK_KEY, callback);
    }

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        // None
    }

    @Override
    public void crash() {
        // None
    }

    private Transaction getTransaction(Session session) {
        return session.get(TXN_KEY); // Note: Assumes 1 session per thread
    }

    private Transaction getAndSetTransaction(Session session) {
        Transaction txn = session.get(TXN_KEY);
        if(txn == null) {
            txn = treeService.getDb().getTransaction();
            session.put(TXN_KEY, txn);
        }
        return txn;
    }

    private void requireInactive(Transaction txn) {
        if(txn.isActive()) {
            throw new IllegalStateException("Transaction already began");
        }
    }

    private void requireActive(Transaction txn) {
        if((txn == null) || !txn.isActive()) {
            throw new IllegalStateException("No transaction open");
        }
    }

    private void end(Session session, Transaction txn, RuntimeException cause) {
        RuntimeException re = cause;
        try {
            txn.end();
            //session.remove(TXN_KEY); // Needed if Sessions ever move between threads
        } catch(RuntimeException e) {
            re = MultipleCauseException.combine(re, e);
        } finally {
            runCallbacks(session, AFTER_END_KEY, -1, re);
        }
    }

    private void runCallbacks(Session session, StackKey<Callback> key, long timestamp, RuntimeException cause) {
        RuntimeException exceptions = cause;
        Callback cb;
        while((cb = session.pop(key)) != null) {
            try {
                cb.run(session, timestamp);
            } catch(RuntimeException e) {
                exceptions = MultipleCauseException.combine(exceptions, e);
            }
        }
        if(exceptions != null) {
            throw exceptions;
        }
    }
}
