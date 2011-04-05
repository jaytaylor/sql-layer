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

package com.akiban.server.service.d_l;

import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DDLReadWriteLockHook implements DStarLFunctionsHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(DDLReadWriteLockHook.class);
    private static final Session.Key<Lock> LOCK_KEY = Session.Key.of("READWRITE_LOCK");
    private static final Session.Key<Boolean> WRITE_LOCK_FAILED = Session.Key.of("WRITE_LOCK_FAILURE");
    static final String IS_LOCK_FAIR_PROPERTY = "akserver.dstarl.lock.fair";

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock( isFair() );

    @Override
    public void hookFunctionIn(Session session, DDLFunction function) {
        assert session.get(WRITE_LOCK_FAILED) == null
                : "WRITE_LOCK_FAILURE not cleared: " + session.get(WRITE_LOCK_FAILED);
        final Lock oldLock;
        if (DStarLFunctionsHook.DStarLType.DDL_FUNCTIONS_WRITE.equals(function.getType())) {
            Lock lock = readWriteLock.writeLock();
            // We have a single-threaded write thread design, so there should never be contention for this lock
            boolean lockWorked = lock.tryLock();
            if (lockWorked) {
                oldLock = session.put(LOCK_KEY, lock);
            } else {
                session.put(WRITE_LOCK_FAILED, true);
                throw new IllegalStateException("write lock has contention!");
            }
        }
        else {
            Lock lock = readWriteLock.readLock();
            oldLock = session.put(LOCK_KEY, lock);
            lock.lock();
        }
        assert oldLock == null : oldLock;
    }

    @Override
    public void hookFunctionCatch(Session session, DDLFunction function, Throwable throwable) {
        // nothing to do
    }

    @Override
    public void hookFunctionFinally(Session session, DDLFunction function) {
        Lock lock = session.remove(LOCK_KEY);
        if (lock == null) {
            if (Boolean.TRUE.equals(session.remove(WRITE_LOCK_FAILED))) {
                return; // exception was already thrown in hookFunctionIn
            }
            String errString = "Lock was null! Some lock has escaped, and this could be a deadlock!";
            LOGGER.error(errString);
            throw new NullPointerException(errString);
        }
        lock.unlock();
    }

    private static boolean isFair() {
        String isFairString = ServiceManagerImpl.get().getConfigurationService().getProperty(IS_LOCK_FAIR_PROPERTY);
        return Boolean.parseBoolean(isFairString);
    }
}
