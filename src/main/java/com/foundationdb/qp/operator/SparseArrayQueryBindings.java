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
import com.foundationdb.qp.util.KeyWrapper;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.util.BloomFilter;
import com.foundationdb.util.SparseArray;
import com.google.common.collect.ArrayListMultimap;

public class SparseArrayQueryBindings implements QueryBindings
{
    private final SparseArray<Object> bindings = new SparseArray<>();
    private final QueryBindings parent;
    private final int depth;

    public SparseArrayQueryBindings() {
        this.parent = null;
        this.depth = 0;
    }

    public SparseArrayQueryBindings(QueryBindings parent) {
        this.parent = parent;
        this.depth = parent.getDepth() + 1;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append('(');
        bindings.describeElements(str);
        if (parent != null) {
            str.append(", ");
            str.append(parent);
        }
        str.append(')');
        return str.toString();
    }

    /* QueryBindings interface */

    @Override
    public ValueSource getValue(int index) {
        if (bindings.isDefined(index)) {
            return (ValueSource)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getValue(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.foundationdb.qp.operator.QueryContext#setValue(int, com.foundationdb.server.types.value.ValueSource)
     * This makes a copy of the ValueSource value, rather than simply
     * storing the reference. The assumption is the ValueSource parameter
     * will be reused by the caller as rows are processed, so the QueryContext
     * needs to keep a copy of the underlying value.
     *
     */
    @Override
    public void setValue(int index, ValueSource value) {
        Value holder = null;
        if (bindings.isDefined(index)) {
            holder = (Value)bindings.get(index);
            if (holder.getType() != value.getType())
                holder = null;
        }
        if (holder == null) {
            holder = new Value(value.getType());
            bindings.set(index, holder);
        }
        ValueTargets.copyFrom(value, holder);
    }
    
    @Override
    public Row getRow(int index) {
        if (bindings.isDefined(index)) {
            return (Row)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getRow(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setRow(int index, Row row)
    {
        bindings.set(index, row);
    }

    @Override
    public HKey getHKey(int index) {
        if (bindings.isDefined(index)) {
            return (HKey)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getHKey(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setHKey(int index, HKey hKey)
    {
        bindings.set(index, hKey);
    }

    @Override
    public BloomFilter getBloomFilter(int index) {
        if (bindings.isDefined(index)) {
            return (BloomFilter)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getBloomFilter(index);
        }
        else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setBloomFilter(int index, BloomFilter filter) {
        bindings.set(index, filter);
    }

    @Override
    public ArrayListMultimap<KeyWrapper, Row> getHashTable(int index){
        if (bindings.isDefined(index)) {
            return (ArrayListMultimap<KeyWrapper, Row>)bindings.get(index);
        }
        else if (parent != null) {
            return parent.getHashTable(index);
        }
         else {
            throw new BindingNotSetException(index);
        }
    }

    @Override
    public void setHashTable(int index, ArrayListMultimap<KeyWrapper, Row> hashTable) {
        bindings.set(index, hashTable);
    }

    @Override
    public void clear() {
        bindings.clear();
    }

    @Override
    public QueryBindings getParent() {
        return parent;
    }

    @Override
    public boolean isAncestor(QueryBindings ancestor) {
        for (QueryBindings descendant = this; descendant != null; descendant = descendant.getParent()) {
            if (descendant == ancestor) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getDepth() {
        return depth;
    }

    @Override
    public QueryBindings createBindings() {
        return new SparseArrayQueryBindings(this);
    }
}
