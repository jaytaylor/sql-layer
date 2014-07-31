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

import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.BloomFilter;
import com.google.common.collect.ArrayListMultimap;

/** The bindings associated with the execution of a query.
 * This includes query parameters (? markers) as well as current values for iteration.
 * More than one QueryBindings may be active at the same time if
 * iteration is being done in parallel for pipelining.
 */
public interface QueryBindings
{
    public ValueSource getValue(int index);

    public void setValue(int index, ValueSource value);

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
     * Gets the hash join table bound to the given index.
     * @param index the index to look up
     * @return the hash join table at that index
     * @throws BindingNotSetException if the given index wasn't set
     */
    public ArrayListMultimap<HashJoin.KeyWrapper, Row> getHashJoinTable(int index);

    /**Bind a hash table hash join table to the given index.
     * @param index the index to set
     * @param hashTable the hash join table to assign
     */
    public void setHashJoinTable(int index, ArrayListMultimap<HashJoin.KeyWrapper, Row> hashTable);

    /**
     * Clear all bindings.
     */
    public void clear();

    /**
     * Get the parent from which undefined bindings inherit.
     */
    public QueryBindings getParent();

    /**
     * Is this bindings an descendant of the given bindings?
     */
    public boolean isAncestor(QueryBindings ancestor);

    /**
     * Get the inheritance depth.
     */
    public int getDepth();

    /**
     * Make a new set of bindings inheriting from this one.
     */
    public QueryBindings createBindings();
}
