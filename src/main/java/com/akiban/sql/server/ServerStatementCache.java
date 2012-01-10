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

package com.akiban.sql.pg;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cache of parsed statements.
 */
public class ServerStatementCache<T extends ServerStatement>
{
    private Cache cache;
    private int hits, misses;

    static class Cache extends LinkedHashMap<String,T> {
        private int capacity;

        public Cache(int capacity) {
            super(capacity, 0.75f, true);
            this.capacity = capacity;
        }
        
        public int getCapacity() {
            return capacity;
        }

        public void setCapacity(int capacity) {
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return (size() > capacity); 
        }  
    }

    public ServerStatementCache(int size) {
        cache = new Cache(size);
    }

    public int getCapacity() {
        return cache.getCapacity();
    }

    public synchronized void setCapacity(int capacity) {
        cache.setCapacity(capacity);
        cache.clear();
    }

    public synchronized T get(String sql) {
        T entry = cache.get(sql);
        if (entry != null)
            hits++;
        else
            misses++;
        return entry;
    }

    public synchronized void put(String sql, T stmt) {
        // TODO: Count number of times this is non-null, meaning that
        // two threads computed the same statement?
        cache.put(sql, stmt);
    }

    public synchronized int getHits() {
        return hits;
    }
    public synchronized int getMisses() {
        return misses;
    }

    public synchronized void invalidate() {
        cache.clear();
    }

    public synchronized void reset() {
        cache.clear();
        hits = 0;
        misses = 0;
    }
}
