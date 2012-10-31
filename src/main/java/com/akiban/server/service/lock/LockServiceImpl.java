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
import com.akiban.server.util.ReadWriteMap;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.akiban.server.util.ReadWriteMap.ValueCreator;

public class LockServiceImpl implements LockService {
    private final static boolean TABLE_LOCK_FAIRENESS = false;

    private final static Session.Key<Boolean> SESSION_HAS_CB_KEY = Session.Key.named("LOCK_HAS_CB");
    private final static Session.MapKey<Integer,int[]> SESSION_SHARED_KEY = Session.MapKey.mapNamed("LOCK_SHARED");
    private final static Session.MapKey<Integer,int[]> SESSION_EXCLUSIVE_KEY = Session.MapKey.mapNamed("LOCK_EXCLUSIVE");

    private final TransactionService txnService;
    private final ReadWriteMap<Integer,ReadWriteLock> lockMap = ReadWriteMap.wrapNonFair(new HashMap<Integer,ReadWriteLock>());

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
        lockMap.clear();
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // LockService methods
    //

    @Override
    public void claimTable(Session session, Mode mode, int tableID) {
        getLevel(mode, getLock(tableID)).lock();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public void claimTableInterruptible(Session session, Mode mode, int tableID) throws InterruptedException {
        getLevel(mode, getLock(tableID)).lock();
        trackForUnlock(session, mode, tableID);
    }

    @Override
    public boolean tryClaimTable(Session session, Mode mode, int tableID, int milliseconds) throws InterruptedException {
        boolean locked = getLevel(mode, getLock(tableID)).tryLock(milliseconds, TimeUnit.MILLISECONDS);
        if(locked) {
            trackForUnlock(session, mode, tableID);
        }
        return locked;
    }

    @Override
    public void releaseTable(Session session, Mode mode, int tableID) {
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

    private ReadWriteLock getLock(int tableID) {
        return lockMap.getOrCreateAndPut(tableID, RWL_CREATOR);
    }

    private Lock getLevel(Mode mode, ReadWriteLock lock) {
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

    private static final ValueCreator<Integer,ReadWriteLock> RWL_CREATOR = new ValueCreator<Integer,ReadWriteLock>() {
        @Override
        public ReadWriteLock createValueForKey(Integer key) {
            return new ReentrantReadWriteLock(TABLE_LOCK_FAIRENESS);
        }
    };
}
