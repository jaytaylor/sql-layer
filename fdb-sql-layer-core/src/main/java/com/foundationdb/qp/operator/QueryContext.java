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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.sql.server.ServerTransaction;

import java.util.Date;

/** The context for the execution of a query.
 * Associated the query with the running environment.
 * The context is global for the whole query; it does not change for
 * different iteration values at different places in the query.
 */
public interface QueryContext
{
    /**
     * Get the store associated with this query.
     */
    public StoreAdapter getStore();
    public StoreAdapter getStore(Table table);

    /**
     * Get the session associated with this context.
     */
    public Session getSession();

    /**
     * Get the service manager.
     */
    public ServiceManager getServiceManager();

    /**
     * Get the current date.
     * This time may be frozen from the start of a transaction.
     */
    public Date getCurrentDate();

    /**
     * Get the current user name.
     */
    public String getCurrentUser();

    /**
     * Get the server user name.
     */
    public String getSessionUser();

    /**
     * Get the system identity of the server process.
     */
    public String getSystemUser();

    /**
     * Get the current schema name.
     */
    public String getCurrentSchema();

    /**
     * Get the current value of a session setting.
     */
    public String getCurrentSetting(String key);

    /**
     * Get the server session id.
     */
    public int getSessionId();

    /**
     * Get the system time at which the query started.
     */
    public long getStartTime();

    /**
     * Possible notification levels for {@link #notifyClient}.
     */
    public enum NotificationLevel {
        WARNING, INFO, DEBUG
    }

    /**
     * Send a warning (or other) notification to the remote client.
     * The message will be delivered as part of the current operation,
     * perhaps immediately or perhaps at its completion, depending on
     * the implementation.
     */
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message);

    /**
     * Send a warning notification to the remote client from the given exception.
     */
    public void warnClient(InvalidOperationException exception);

    /** Get the query timeout in milliseconds or <code>-1</code> if no limit. */
    public long getQueryTimeoutMilli();

    /** Check whether query has been cancelled or timeout has been exceeded. */
    public void checkQueryCancelation();

    /** Does this context commit periodically? */
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit();

    /**
     * Create a new empty set of bindings.
     */
    public QueryBindings createBindings();
}
