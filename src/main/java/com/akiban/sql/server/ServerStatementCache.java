/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.server;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cache of parsed statements.
 */
public class ServerStatementCache<T extends ServerStatement>
{
    private final CacheCounters counters;
    private final Cache<T> cache;

    static class Cache<T> extends LinkedHashMap<String,T> {
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

    public ServerStatementCache(CacheCounters counters, int size) {
        this.counters = counters;
        this.cache = new Cache<T>(size);
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
            counters.incrementHits();
        else
            counters.incrementMisses();
        return entry;
    }

    public synchronized void put(String sql, T stmt) {
        // TODO: Count number of times this is non-null, meaning that
        // two threads computed the same statement?
        cache.put(sql, stmt);
    }

    public synchronized void invalidate() {
        cache.clear();
    }

    public synchronized void reset() {
        cache.clear();
    }
}
