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

package com.foundationdb.server.service.transaction;

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.session.Session;

import java.util.concurrent.Callable;

public interface TransactionService extends Service {
    interface Callback {
        void run(Session session, long timestamp);
    }

    interface CloseableTransaction extends AutoCloseable {
        void commit();
        void rollback();
        boolean commitOrRetry();
        @Override
        void close();
    }

    enum CallbackType {
        /** Invoked prior to calling commit. */
        PRE_COMMIT,
        /** Invoked <i>after</i> commit completes successfully. */
        COMMIT,
        /** Invoked <i>after</i> rollback completes successfully (but not for commit failure). */
        ROLLBACK,
        /** Invoked when the transaction ends, independent of success/failure of commit/rollback. */
        END
    }

    /** Returns true if there is a transaction active for the given Session */
    boolean isTransactionActive(Session session);

    /** Returns true if there is a transaction active for the given Session */
    boolean isRollbackPending(Session session);

    /** Returns the start timestamp for the open transaction. */
    long getTransactionStartTimestamp(Session session);

    /** Begin a new transaction. */
    void beginTransaction(Session session);

    /** Begin a new transaction that will rollback upon close if not committed. */
    CloseableTransaction beginCloseableTransaction(Session session);

    /** Commit the open transaction. */
    void commitTransaction(Session session);

    /** Commit the open transaction or reset and reopen it if a retryable rollback exception occurs.
     * @return <code>true</code> if the caller should retry.
     */
    boolean commitOrRetryTransaction(Session session);

    /** Rollback an open transaction. */
    void rollbackTransaction(Session session);

    /** Rollback the current transaction if open, otherwise do nothing. */
    void rollbackTransactionIfOpen(Session session);

    /** Commit the transaction if this is a good time. Returns {@code true} if a commit was performed. */
    boolean periodicallyCommit(Session session);

    /** Is this a good time for commit? Returns {@code true} if a commit should be performed. */
    boolean periodicallyCommitNow(Session session);

    /** Add a callback to transaction. */
    void addCallback(Session session, CallbackType type, Callback callback);

    /** Add a callback to transaction that is required to be active. */
    void addCallbackOnActive(Session session, CallbackType type, Callback callback);

    /** Add a callback to transaction that is required to be inactive. */
    void addCallbackOnInactive(Session session, CallbackType type, Callback callback);

    /** Wrap <code>runnable</code> in a <code>Callable</code> and invoke {@link #run(Session, Callable)}. */
    void run(Session session, Runnable runnable);

    /**
     * Execute in a new transaction and automatically retry if a rollback exception occurs.
     * <p>Note: A plain <code>Exception</code> from <code>callable</code> will be <i>rethrown</i>, not retried.</p>
     */
    <T> T run(Session session, Callable<T> callable);

    enum SessionOption { 
        /** Control when / how constraints like uniqueness are checked. */
        CONSTRAINT_CHECK_TIME
    }

    /** Set user option on <code>Session</code>. */
    void setSessionOption(Session session, SessionOption option, String value);

    /** If a transaction can fail with a rollback transaction after
     * actually having succeeded, store a unique counter associated
     * with the given session persistently so that it can be checked.
     */
    int markForCheck(Session session);

    /** Check for success from given counter stored in previous
     * transaction that failed due with the given exception.
     */
    boolean checkSucceeded(Session session, Exception retryException, int sessionCounter);

    /** Defer some foreign key checks within this transaction. */
    void setDeferredForeignKey(Session session, ForeignKey foreignKey, boolean deferred);

    /** Repeat any checks that are scoped to the statement. */
    void checkStatementForeignKeys(Session session);
}
