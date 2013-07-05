/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;

import java.util.Date;

/** The context for the execution of a query.
 * Associated the query with the running environment.
 * The context is global for the whole query; it does not change for
 * different iteration values at different places in the query.
 */
public interface QueryContext extends QueryBindings
{
    /**
     * Get the store associated with this query.
     */
    public StoreAdapter getStore();
    public StoreAdapter getStore(UserTable table);

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

    /** Check constraints on row.
     * @throws InvalidOperationException thrown if a constraint on the row is violated.
     */
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException;    
    /**
     * Get the next value for the named Sequence. 
     * @throws NoSuchSequenceException if the name does not exist in the system.  
     */
    public long sequenceNextValue(TableName sequence); 
    /**
     * Get the current value for the named Sequence. 
     * @throws NoSuchSequenceException if the name does not exist in the system.  
     */
    public long sequenceCurrentValue(TableName sequence); 
}
