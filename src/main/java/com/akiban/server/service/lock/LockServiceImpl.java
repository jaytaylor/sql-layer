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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockServiceImpl implements LockService {
    private final static boolean LOCK_MAP_LOCK_FAIRNESS = false;
    private final static boolean TABLE_LOCK_FAIRENESS = false;

    private final static Session.Key<Boolean> SESSION_HAS_CB_KEY = Session.Key.named("LOCK_HAS_CB");
    private final static Session.MapKey<Integer,int[]> SESSION_SHARED_KEY = Session.MapKey.mapNamed("LOCK_SHARED");
    private final static Session.MapKey<Integer,int[]> SESSION_EXCLUSIVE_KEY = Session.MapKey.mapNamed("LOCK_EXCLUSIVE");

    private final TransactionService txnService;
    private final Map<Object,ReentrantReadWriteLock> lockMap = new HashMap<Object, ReentrantReadWriteLock>();
    private final ReentrantReadWriteLock lockMapLock = new ReentrantReadWriteLock(LOCK_MAP_LOCK_FAIRNESS);

    private final TransactionService.Callback unlockCallback = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            unlockEntireSessionMap(session, Mode.SHARED, SESSION_SHARED_KEY);
            unlockEntireSessionMap(session, Mode.EXCLUSIVE, SESSION_EXCLUSIVE_KEY);
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
    public void tableClaim(Session session, Mode mode, int tableID) {
        getLevel(mode, getLock(tableID)).lock();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public void tableClaimInterruptible(Session session, Mode mode, int tableID) throws InterruptedException {
        getLevel(mode, getLock(tableID)).lock();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public boolean tableTryClaim(Session session, Mode mode, int tableID, int milliseconds) throws InterruptedException {
        boolean locked = getLevel(mode, getLock(tableID)).tryLock(milliseconds, TimeUnit.MILLISECONDS);
        if(locked) {
            trackForUnlock(session, mode, tableID);
        }
        return locked;
    }

    @Override
    public void tableRelease(Session session, Mode mode, int tableID) {
        int[] count = getLockedCount(session, mode, tableID);
        if((count == null) || (count[0] == 0)) {
            throw new IllegalArgumentException("Table is not locked: " + tableID);
        }
        getLevel(mode, getLock(tableID)).unlock();
        --count[0];
    }


    //
    // Internal methods
    //

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

    private Lock getLevel(Mode mode, ReentrantReadWriteLock lock) {
        switch(mode) {
            case SHARED:
                return lock.readLock();
            case EXCLUSIVE:
                return lock.writeLock();
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }
    }

    private Map<Integer,int[]> getOrCreateModeMap(Session session, Mode mode) {
        Map<Integer,int[]> map = session.get(getMapKey(mode));
        if(map == null) {
            map = new HashMap<Integer,int[]>();
            session.put(getMapKey(mode), map);
        }
        return map;
    }

    private int[] getLockedCount(Session session, Mode mode, int tableID) {
        return getOrCreateModeMap(session, mode).get(tableID);
    }

    private void trackForUnlock(Session session, Mode mode, int tableID) {
        Map<Integer,int[]> map = getOrCreateModeMap(session, mode);
        int[] count = map.get(tableID);
        if(count == null) {
            count = new int[1];
            map.put(tableID, count);
        }
        ++count[0];
        Boolean hasCB = session.get(SESSION_HAS_CB_KEY);
        if(hasCB == null) {
            session.put(SESSION_HAS_CB_KEY, Boolean.TRUE);
            txnService.addEndCallback(session, unlockCallback);
        }
    }

    private void unlockEntireSessionMap(Session session, Mode mode, Session.MapKey<Integer, int[]> key) {
        Map<Integer,int[]> lockedTables = session.remove(key);
        if(lockedTables != null) {
            for(Map.Entry<Integer, int[]> entry : lockedTables.entrySet()) {
                Lock lock = getLevel(mode, getLock(entry.getKey()));
                final int lockCount = entry.getValue()[0];
                for(int i = 0; i < lockCount; ++i) {
                    lock.unlock();
                }
            }
        }
    }

    private static Session.MapKey<Integer,int[]> getMapKey(Mode mode) {
        switch(mode) {
            case SHARED:
                return SESSION_SHARED_KEY;
            case EXCLUSIVE:
                return SESSION_EXCLUSIVE_KEY;
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }
    }
}
