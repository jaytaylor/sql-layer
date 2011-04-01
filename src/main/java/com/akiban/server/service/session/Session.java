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

import java.util.Map;

public interface Session
{
    <T> T get(Key<T> key);
    <T> T put(Key<T> key, T item);
    <T> T remove(Key<T> key);

    <K,V> V get(MapKey<K,V> mapKey, K key);
    <K,V> V put(MapKey<K,V> mapKey, K key, V value);
    <K,V> V remove(MapKey<K,V> mapKey, K key);

    /**
     * Closes all the resources managed by this session.
     */
    void close();

    public static class Key<T> {
        private final String name;
        private final T defaultValue;

        public static <T> Key<T> of(String name) {
            return new Key<T>(name, null, 1);
        }

        public static <T> Key<T> of(String name, T defaultValue) {
            return new Key<T>(name, defaultValue, 1);
        }

        private Key(String name, T defaultValue, int stackFramesToOwner) {
            Class<?> owner = Thread.currentThread().getStackTrace()[stackFramesToOwner + 1].getClass();
            this.name = String.format("%s<%s>", owner.getSimpleName(), name);
            this.defaultValue = defaultValue;
        }

        @Override
        public String toString() {
            return name;
        }

        public T getDefaultValue() {
            return defaultValue;
        }
    }

    public static final class MapKey<K,V> extends Key<Map<K,V>> {
        private final V defaultValue;

        public static <K,V> MapKey<K,V> ofMap(String name) {
            return new MapKey<K,V>(name, null);
        }

        public static <K,V> MapKey<K,V> ofMap(String name, V defaultValue) {
            return new MapKey<K,V>(name, defaultValue);
        }

        private MapKey(String name, V defaultValue) {
            super(name, null, 3);
            this.defaultValue = defaultValue;
        }

        public V getDefaultMapValue() {
            return defaultValue;
        }

        Key<Map<K,V>> asKey() {
            return this;
        }
    }
}
