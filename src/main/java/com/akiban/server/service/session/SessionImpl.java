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

package com.akiban.server.service.session;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public final class SessionImpl implements Session
{
    private static final Object NULL_OBJ = new Object();
    private final Map<Key<?>,Object> map = new HashMap<Key<?>, Object>();

    @Override
    public <T> T get(Session.Key<T> key) {
        return launder(map.get(key));
    }

    @Override
    public <T> T put(Session.Key<T> key, T item) {
        return launder(map.put(key, item == null ? NULL_OBJ : item));
    }

    @Override
    public <T> T remove(Key<T> key) {
        return launder(map.remove(key));
    }

    @Override
    public <K,V> V get(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    @Override
    public <K,V> V put(MapKey<K,V> mapKey, K key, V value) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            map = new HashMap<K, V>();
            put(mapKey.asKey(), map);
        }
        return map.put(key, value);
    }

    @Override
    public <K,V> V remove(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.remove(key);
    }

    @Override
    public <T> void push(StackKey<T> key, T item) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            deque = new ArrayDeque<T>();
            put(key.asKey(), deque);
        }
        deque.offerLast(item);
    }

    @Override
    public <T> T pop(StackKey<T> key) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            return null;
        }
        return deque.pollLast();
    }

    private static <T> T launder(Object o) {
        @SuppressWarnings("unchecked") T t = (T) o;
        if (t == null) {
            return null;
        }
        return t == NULL_OBJ ? null : t;
    }

    @Override
    public void close()
    {
        // For now do nothing to any cached resources.
        // Later, we'll close any "resource" that is added to the session.
        //
        map.clear();
    }
}
