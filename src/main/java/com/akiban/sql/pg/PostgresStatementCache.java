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

import com.akiban.sql.StandardException;

import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cache of parsed statements.
 */
public class PostgresStatementCache
{
    private Map<String,PostgresStatement> cache;
    private int hits, misses;

    static class Cache extends LinkedHashMap<String,PostgresStatement> {
        private int capacity;

        public Cache(int capacity) {
            super(capacity, 0.75f, true);
            this.capacity = capacity;
        }

        protected boolean removeEldestEntry(Map.Entry eldest) {  
            return (size() > capacity); 
        }  
    }

    public PostgresStatementCache(int size) {
        cache = new Cache(size);
    }

    public synchronized PostgresStatement get(String sql) {
        PostgresStatement entry = cache.get(sql);
        if (entry != null)
            hits++;
        else
            misses++;
        return entry;
    }

    public synchronized void put(String sql, PostgresStatement stmt) {
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

    public synchronized void reset() {
        cache.clear();
        hits = 0;
        misses = 0;
    }

}
