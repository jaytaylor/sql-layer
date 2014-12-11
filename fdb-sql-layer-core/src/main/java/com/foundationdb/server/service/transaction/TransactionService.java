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
import com.foundationdb.sql.parser.IsolationLevel;

import java.util.concurrent.Callable;

public interface TransactionService extends Service {
    interface Callback {
        void run(Session session, long timestamp);
    }

    interface CloseableTransaction extends AutoCloseable {
        void commit();
        void rollback();
        @Override
        void close();
    }

    enum CallbackType {
        /** Invoked <i>before</i> attempting to commit. */
        PRE_COMMIT,
        /** Invoked <i>after</i> commit completes successfully. */
        COMMIT,
        /** Invoked <i>after</i> a commit fails or rollback is manually invoked. */
        ROLLBACK,
        /** Invoked <i>after</i> a transaction ends, either by commit or rollback. */
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

    /**
     * Commit the transaction and reset for immediate use if a retryable exception occurs.
     *
     * <p>
     *     If the commit was successful <i>or</i> a non-retryable exception occurred, the
     *     session's transaction state is cleared and {@link #beginTransaction} must be
     *     called before further use. A non-retryable exception is immediately propagated.
     *     Otherwise, the state is reset and ready for use.
     * </p>
     *
     * @return <code>true</code> if the caller should retry.
     */
    boolean commitOrRetryTransaction(Session session);

    /** Rollback an open transaction. */
    void rollbackTransaction(Session session);

    /** Rollback the current transaction if open, otherwise do nothing. */
    void rollbackTransactionIfOpen(Session session);

    /**
     * Commit the transaction if this is a good time.
     *
     * <p>
     *     If the commit was successful, the transaction state is reset and ready
     *     for reuse. Otherwise, the exception is propagated and
     *     {@link #rollbackTransaction} must be called be called before reuse.
     * </p>
     *
     * @return {@code true} if a commit was performed, false otherwise
     */
    boolean periodicallyCommit(Session session);

    /** @return {@code true} if this is a good time to commit, {@code false} otherwise */
    boolean shouldPeriodicallyCommit(Session session);

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

    /** Check if checks should be performed immediately. */
    boolean getForceImmediateForeignKeyCheck(Session session);

    /** Set if checks should be performed immediately (true disables all deferring). Return previous value. */
    boolean setForceImmediateForeignKeyCheck(Session session, boolean force);

    /** Return the isolation level that would be in effect if the given one were requested. */
    IsolationLevel actualIsolationLevel(IsolationLevel level);

    /** Set isolation level for the session's transaction. */
    IsolationLevel setIsolationLevel(Session session, IsolationLevel level);

    /** Does this isolation level only work with read-only transactions? */
    boolean isolationLevelRequiresReadOnly(Session session, boolean commitNow);

}
