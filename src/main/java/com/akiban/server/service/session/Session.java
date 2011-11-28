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

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public final class Session
{
    private final static AtomicLong idGenerator = new AtomicLong(0);

    private final Map<Key<?>,Object> map = new HashMap<Key<?>, Object>();
    private final SessionEventListener listener;
    private final long sessionId = idGenerator.getAndIncrement();
    private volatile boolean cancelCurrentQuery = false;

    public String toString()
    {
        return String.format("Session(%s)", sessionId);
    }

    Session(SessionEventListener listener) {
        this.listener = listener;
    }

    public long sessionId() {
        return sessionId;
    }

    public <T> T get(Session.Key<T> key) {
        return cast(key, map.get(key));
    }

    public <T> T put(Session.Key<T> key, T item) {
        return cast(key, map.put(key, item));
    }

    public <T> T remove(Key<T> key) {
        return cast(key, map.remove(key));
    }

    public <K,V> V get(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    public <K,V> V put(MapKey<K,V> mapKey, K key, V value) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            map = new HashMap<K, V>();
            put(mapKey.asKey(), map);
        }
        return map.put(key, value);
    }

    public <K,V> V remove(MapKey<K,V> mapKey, K key) {
        Map<K,V> map = get( mapKey.asKey() );
        if (map == null) {
            return null;
        }
        return map.remove(key);
    }

    public <T> void push(StackKey<T> key, T item) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            deque = new ArrayDeque<T>();
            put(key.asKey(), deque);
        }
        deque.offerLast(item);
    }

    public <T> T pop(StackKey<T> key) {
        Deque<T> deque = get( key.asKey() );
        if (deque == null) {
            return null;
        }
        return deque.pollLast();
    }

    // "unused" suppression: Key<T> is only used for type inference
    // "unchecked" suppression: we know from the put methods that Object will be of type T
    @SuppressWarnings({"unused", "unchecked"})
    private static <T> T cast(Key<T> key, Object o) {
        return (T) o;
    }

    public void close()
    {
        if (listener != null) {
            listener.sessionClosing();
        }
        // For now do nothing to any cached resources.
        // Later, we'll close any "resource" that is added to the session.
        //
        map.clear();
    }

    public void cancelCurrentQuery(boolean cancel)
    {
        cancelCurrentQuery = cancel;
    }

    public boolean isCurrentQueryCanceled()
    {
        return cancelCurrentQuery;
    }

    @SuppressWarnings("unused") // for <T> parameter; it's only useful for compile-time checking
    public static class Key<T> {
        private final Class<?> owner;
        private final String name;

        public static <T> Key<T> named(String name) {
            return new Key<T>(name, 1);
        }

        private Key(String name, int stackFramesToOwner) {
            try {
                owner = Class.forName(Thread.currentThread().getStackTrace()[stackFramesToOwner + 2].getClassName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            this.name = String.format("%s<%s>", owner.getSimpleName(), name);
        }

        @Override
        public String toString() {
            return name;
        }

        Class<?> getOwner() {
            return owner;
        }
    }

    public static final class MapKey<K,V> extends Key<Map<K,V>> {

        public static <K,V> MapKey<K,V> mapNamed(String name) {
            return new MapKey<K,V>(name);
        }

        private MapKey(String name) {
            super(name, 3);
        }

        Key<Map<K,V>> asKey() {
            return this;
        }
    }

    public static final class StackKey<T> extends Key<Deque<T>> {

        public static <K> StackKey<K> stackNamed(String name) {
            return new StackKey<K>(name);
        }

        private StackKey(String name) {
            super(name, 3);
        }

        public Key<Deque<T>> asKey() {
            return this;
        }
    }
}
