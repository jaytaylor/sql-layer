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

package com.akiban.qp.operator;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.service.session.Session;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.BloomFilter;

import java.util.Date;

public interface QueryContext 
{
    public PValueSource getPValue(int index);

    public void setPValue(int index, PValueSource value);

    /**
     * Gets the value bound to the given index.
     * @param index the index to look up
     * @return the value at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public ValueSource getValue(int index);

    /**
     * Bind a value to the given index.
     * @param index the index to set
     * @param value the value to assign
     */
    public void setValue(int index, ValueSource value);

    /**
     * Bind a value to the given index.
     * @param index the index to set
     * @param value the value to assign
     * @param type the type to convert the value to for binding
     */
    public void setValue(int index, ValueSource value, AkType type);

    /**
     * Gets the row bound to the given index.
     * @param index the index to look up
     * @return the row at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public Row getRow(int index);

    /**
     * Bind a row to the given index.
     * @param index the index to set
     * @param row the row to assign
     */
    public void setRow(int index, Row row);

    /**
     * Gets the hKey bound to the given index.
     * @param index the index to look up
     * @return the hKey at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public HKey getHKey(int index);

    /**
     * Bind an hkey to the given index.
     * @param index the index to set
     * @param hKey the hKey to assign
     */
    public void setHKey(int index, HKey hKey);

    /**
     * Gets the bloom filter bound to the given index.
     * @param index the index to look up
     * @return the bloom filter at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public BloomFilter getBloomFilter(int index);

    /**
     * Bind a bloom filter to the given index.
     * @param index the index to set
     * @param filter the bloom filter to assign
     */
    public void setBloomFilter(int index, BloomFilter filter);

    /**
     * Clear all bindings.
     */
    public void clear();

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

    /** Get the query timeout in seconds or <code>-1</code> if no limit. */
    public long getQueryTimeoutSec();

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
