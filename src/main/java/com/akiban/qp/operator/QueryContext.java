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

package com.akiban.qp.operator;

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.service.session.Session;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.util.Date;

public interface class QueryContext 
{
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
     * Bind a value to the given index.
     * @param index the index to set
     * @param value the value to assign
     */
    public void setValue(int index, Object value);

    /**
     * Bind a value to the given index.
     * @param index the index to set
     * @param value the value to assign
     */
    public void setValue(int index, Object value, AkType type);

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
     * Get the store associated with this query.
     */
    public StoreAdapter getStore();

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
     * Get the current schema name.
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
     * Get the system time at which the query started.
     */
    public long getStartTime();
}
