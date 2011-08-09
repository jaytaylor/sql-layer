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

package com.akiban.server.service.dxl;

import com.akiban.server.service.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class DXLReadWriteLockHook implements DXLFunctionsHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(DXLReadWriteLockHook.class);
    private static final Session.StackKey<Lock> LOCK_KEY = Session.StackKey.stackNamed("READWRITE_LOCK");
    private static final Session.Key<Boolean> WRITE_LOCK_TAKEN = Session.Key.named("WRITE_LOCK_TAKEN");
    static final String WRITE_LOCK_TAKEN_MESSAGE = "Another thread has the write lock! Writes are supposed to be single-threaded";

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    private final static DXLReadWriteLockHook INSTANCE = new DXLReadWriteLockHook();

    public static DXLReadWriteLockHook only() {
        return INSTANCE;
    }

    private DXLReadWriteLockHook() {
        // Having multiple of these introduces the possibility of a deadlock, for all the usual deadlocky reasons
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        final Lock lock;
        if (DXLType.DDL_FUNCTIONS_WRITE.equals(function.getType())) {
            if (readWriteLock.isWriteLocked() && (!readWriteLock.isWriteLockedByCurrentThread())) {
                session.put(WRITE_LOCK_TAKEN, true);
                throw new IllegalStateException(WRITE_LOCK_TAKEN_MESSAGE);
            }
            lock = readWriteLock.writeLock();
        }
        else {
            lock = readWriteLock.readLock();
        }
        session.push(LOCK_KEY, lock);
        lock.lock();
    }

    @Override
    public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
        // nothing to do
    }

    @Override
    public void hookFunctionFinally(Session session, DXLFunction function, Throwable t) {
        Lock lock = session.pop(LOCK_KEY);
        if (lock == null) {
            Boolean writeLockWasTaken = session.remove(WRITE_LOCK_TAKEN);
            if (writeLockWasTaken != null && writeLockWasTaken) {
                return; // assertion was thrown
            }
            throw new LockNotSetException(t);
        }
        lock.unlock();
    }

    private static class LockNotSetException extends RuntimeException {
        private static final String ERR_STRING = "Lock was null! Some lock has escaped, and this could be a deadlock!";
        private LockNotSetException(Throwable cause) {
            super(ERR_STRING, cause);
            LOGGER.error(ERR_STRING);
        }
    }
}
