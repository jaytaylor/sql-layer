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

import java.util.Deque;
import java.util.Map;

public interface Session
{
    <T> T get(Key<T> key);
    <T> T put(Key<T> key, T item);
    <T> T remove(Key<T> key);

    <K,V> V get(MapKey<K,V> mapKey, K key);
    <K,V> V put(MapKey<K,V> mapKey, K key, V value);
    <K,V> V remove(MapKey<K,V> mapKey, K key);

    <T> void push(StackKey<T> key, T item);
    <T> T pop(StackKey<T> key);

    /**
     * Closes all the resources managed by this session.
     */
    void close();

    public static class Key<T> {
        private final Class<?> owner;
        private final String name;
        private final T defaultValue;

        public static <T> Key<T> named(String name) {
            return new Key<T>(name, null, 1);
        }

        public static <T> Key<T> named(String name, T defaultValue) {
            return new Key<T>(name, defaultValue, 1);
        }

        private Key(String name, T defaultValue, int stackFramesToOwner) {
            try {
                owner = Class.forName(Thread.currentThread().getStackTrace()[stackFramesToOwner + 2].getClassName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
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

        Class<?> getOwner() {
            return owner;
        }
    }

    public static final class MapKey<K,V> extends Key<Map<K,V>> {

        public static <K,V> MapKey<K,V> mapNamed(String name) {
            return new MapKey<K,V>(name);
        }

        private MapKey(String name) {
            super(name, null, 3);
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
            super(name, null, 3);
        }

        public Key<Deque<T>> asKey() {
            return this;
        }
    }
}
