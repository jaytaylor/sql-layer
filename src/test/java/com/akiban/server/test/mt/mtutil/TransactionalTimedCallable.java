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

package com.akiban.server.test.mt.mtutil;

import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import java.util.concurrent.atomic.AtomicReference;

public final class TransactionalTimedCallable<T> extends TimedCallable<T> {

    private enum Mode {
        WITH_RUNNABLE,
        WITHOUT_RUNNABLE
    }

    private final TimedCallable<? extends T> wrapped;
    private final Mode mode;
    private final int retryCount;
    private final long retryDelay;

    @SuppressWarnings("unused")
    public static <T> TransactionalTimedCallable<T> withRunnable(TimedCallable<? extends T> wrapped,
                                                                 int retryCount, long retryDelay)
    {
        return new TransactionalTimedCallable<T>(wrapped, retryCount, retryDelay);
    }

    @SuppressWarnings("unused")
    public static <T> TransactionalTimedCallable<T> withoutRunnable(TimedCallable<? extends T> wrapped) {
        return new TransactionalTimedCallable<T>(wrapped);
    }

    private TransactionalTimedCallable(TimedCallable<? extends T> wrapped) {
        this.wrapped = wrapped;
        this.mode = Mode.WITHOUT_RUNNABLE;
        this.retryDelay = this.retryCount = -1;
    }

    private TransactionalTimedCallable(TimedCallable<? extends T> wrapped, int retryCount, long retryDelay) {
        this.wrapped = wrapped;
        this.mode = Mode.WITH_RUNNABLE;
        this.retryCount = retryCount;
        this.retryDelay = retryDelay;
    }

    @Override
    protected T doCall(TimePoints timePoints, Session session) throws Exception {
        Transaction transaction = ServiceManagerImpl.get().getTreeService().getTransaction(session);
        switch (mode) {
            case WITH_RUNNABLE:    return withRunnable(timePoints, session, transaction);
            case WITHOUT_RUNNABLE: return withoutRunnable(timePoints, session, transaction);
            default: throw new AssertionError(mode);
        }
    }

    private T withoutRunnable(TimePoints timePoints, Session session, Transaction transaction) throws Exception {
        final T result;
        try {
            transaction.begin();
            result = wrapped.doCall(timePoints, session);
            transaction.commit();
        } finally {
            transaction.end();
        }
        return result;
    }

    private T withRunnable(final TimePoints timePoints, final Session session, Transaction transaction) throws Exception {
        final AtomicReference<T> resultReference = new AtomicReference<T>();
        transaction.run(
                new TransactionRunnable() {
                    @Override
                    public void runTransaction() throws PersistitException, RollbackException {
                        try {
                            T result = wrapped.doCall(timePoints, session);
                            resultReference.set(result);
                        } catch (Exception e) {
                            throw new RollbackException(e);
                        }
                    }
                },
                retryCount,
                retryDelay,
                false
        );
        return resultReference.get();
    }
}
