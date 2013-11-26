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

package com.foundationdb.server.util;

import java.util.LinkedHashMap;
import java.util.Map;

/** A {@link java.util.LinkedHashMap} that overrides default methods, as described in the Javadoc, for LRU behavior. */
public class LRUCacheMap<K,V> extends LinkedHashMap<K,V>
{
    // Same as HashMap
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    // For LRU behavior
    private static final boolean LINKED_ORDER = true;

    private int capacity;

    public LRUCacheMap(int capacity) {
        this(capacity, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public LRUCacheMap(int capacity, int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor, LINKED_ORDER);
        this.capacity = capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean removeEldestEntry(Map.Entry entry) {
        return size() > capacity;
    }
}
