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

package com.akiban.server.service.lock;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockServiceImpl implements LockService {
    private final static boolean TXN_LOCK_FAIRNESS = false;
    private final static boolean LOCK_MAP_LOCK_FAIRNESS = false;
    private final static boolean TABLE_LOCK_FAIRENESS = false;

    private final Session.Key<Boolean> SESSION_HAS_CB_KEY = Session.Key.named("LOCK_HAS_CB");
    private final Session.MapKey<Integer,Mode> SESSION_TABLES_KEY = Session.MapKey.mapNamed("LOCK_TABLES");

    private final TransactionService txnService;
    private final Map<Object,ReentrantReadWriteLock> lockMap = new HashMap<Object, ReentrantReadWriteLock>();
    private final ReentrantReadWriteLock txnLock = new ReentrantReadWriteLock(TXN_LOCK_FAIRNESS);
    private final ReentrantReadWriteLock lockMapLock = new ReentrantReadWriteLock(LOCK_MAP_LOCK_FAIRNESS);

    private final TransactionService.Callback unlockCallback = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            Iterator<Map.Entry<Integer,Mode>> it = session.iterator(SESSION_TABLES_KEY);
            while(it.hasNext()) {
                Map.Entry<Integer, Mode> entry = it.next();
                tableRelease(session, entry.getValue(), entry.getKey(), false);
            }
            session.remove(SESSION_TABLES_KEY);
            session.remove(SESSION_HAS_CB_KEY);
        }
    };


    @Inject
    public LockServiceImpl(TransactionService txnService) {
        this.txnService = txnService;
    }


    //
    // Service methods
    //

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        lockMapLock.writeLock().lock();
        try {
            lockMap.clear();
        } finally {
            lockMapLock.writeLock().unlock();
        }
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // LockService methods
    //

    @Override
    public void transactionClaim(Session session, Mode mode) {
        getAccess(mode, txnLock).lock();
    }

    @Override
    public void transactionClaimInterruptible(Session session, Mode mode) throws InterruptedException {
        getAccess(mode, txnLock).lockInterruptibly();
    }

    @Override
    public boolean transactionTryClaim(Session session, Mode mode, int milliseconds) throws InterruptedException {
        return getAccess(mode, txnLock).tryLock(milliseconds, TimeUnit.MILLISECONDS);
    }

    @Override
    public void transactionRelease(Session session, Mode mode) {
        getAccess(mode, txnLock).unlock();
    }

    @Override
    public void tableClaim(Session session, Mode mode, int tableID) {
        getAccess(mode, getLock(tableID)).lock();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public void tableClaimInterruptible(Session session, Mode mode, int tableID) throws InterruptedException {
        getAccess(mode, getLock(tableID)).lockInterruptibly();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public boolean tableTryClaim(Session session, Mode mode, int tableID, int milliseconds) throws InterruptedException {
        boolean locked = getAccess(mode, getLock(tableID)).tryLock(milliseconds, TimeUnit.MILLISECONDS);
        if(locked) {
            trackForUnlock(session, mode, tableID);
        }
        return locked;
    }

    @Override
    public void tableRelease(Session session, Mode mode, int tableID) {
        tableRelease(session, mode, tableID, true);
    }

    //
    // Internal methods
    //

    public void tableRelease(Session session, Mode mode, int tableID, boolean removeKey) {
        Mode prevMode = removeKey ? session.remove(SESSION_TABLES_KEY, tableID) : session.get(SESSION_TABLES_KEY, tableID);
        if(prevMode == null) {
            throw new IllegalArgumentException("Table is not locked: " + tableID);
        }
        if(prevMode != mode) {
            throw new IllegalAccessError("Attempt to unlock " + prevMode + " with " + mode + " for table: " + tableID);
        }
        getAccess(mode, getLock(tableID)).unlock();
    }

    private ReentrantReadWriteLock getLock(int tableID) {
        ReentrantReadWriteLock lock;
        lockMapLock.readLock().lock();
        try {
            lock = lockMap.get(tableID);
        } finally {
            lockMapLock.readLock().unlock();
        }
        if(lock == null) {
            lockMapLock.writeLock().lock();
            try {
                lock = lockMap.get(tableID);
                if(lock == null) {
                    lock = new ReentrantReadWriteLock(TABLE_LOCK_FAIRENESS);
                    lockMap.put(tableID, lock);
                }
            } finally {
                lockMapLock.writeLock().unlock();
            }
        }
        return lock;
    }

    private Lock getAccess(Mode mode, ReentrantReadWriteLock lock) {
        switch(mode) {
            case SHARED:
                return lock.readLock();
            case EXCLUSIVE:
                return lock.writeLock();
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }
    }

    private void trackForUnlock(Session session, Mode mode, int tableID) {
        session.put(SESSION_TABLES_KEY, tableID, mode);
        Boolean hasCB = session.get(SESSION_HAS_CB_KEY);
        if(hasCB == null) {
            session.put(SESSION_HAS_CB_KEY, Boolean.TRUE);
            txnService.addEndCallback(session, unlockCallback);
        }
    }
}
