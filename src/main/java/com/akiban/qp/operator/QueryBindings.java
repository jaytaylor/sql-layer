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

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.BloomFilter;

/** The bindings associated with the execution of a query.
 * This includes query parameters (? markers) as well as current values for iteration.
 * More than one QueryBindings may be active at the same time if
 * iteration is being done in parallel for pipelining.
 */
public interface QueryBindings
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
}
