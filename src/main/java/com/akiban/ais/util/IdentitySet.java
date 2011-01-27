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

package com.akiban.ais.util;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

public class IdentitySet<T> implements Set<T>
{
    // Set interface

    @Override
    public boolean equals(Object obj)
    {
        boolean eq;
        if (obj instanceof IdentitySet) {
            IdentitySet<T> that = (IdentitySet<T>) obj;
            if (this.size() == that.size()) {
                eq = true;
                Set<T> thatKeys = that.map.keySet();
                for (Iterator<T> i = map.keySet().iterator(); eq && i.hasNext();) {
                    eq = thatKeys.contains(i.next());
                }
            } else {
                eq = false;
            }
        } else {
            eq = false;
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        assert false : "Don't put mutable objects in a hash table";
        return 0;
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o)
    {
        return map.containsKey(o);
    }

    @Override
    public Iterator<T> iterator()
    {
        return map.keySet().iterator();
    }

    @Override
    public Object[] toArray()
    {
        return map.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        return map.keySet().toArray(a);
    }

    @Override
    public boolean add(T t)
    {
        return map.put(t, t) == null;
    }

    @Override
    public boolean remove(Object o)
    {
        return map.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        return map.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        map.clear();
    }

    // IdentitySet interface

    public IdentitySet()
    {}

    public IdentitySet(Collection<T> c)
    {
        for (T t : c) {
            map.put(t, t);
        }
    }

    // State

    private final IdentityHashMap<T, T> map = new IdentityHashMap<T, T>();
}
