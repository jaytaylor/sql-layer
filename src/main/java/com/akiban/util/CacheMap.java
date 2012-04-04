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

package com.akiban.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>A caching map based on a LRU algorithm. In addition to providing caches, this map also lets you define default
 * values for keys. If you do so (by defining an Allocator), a <tt>get</tt> for a key K that misses the cache will
 * generate the value V, update the map to insert (K,V) and return V. If you don't define an allocator, a cache
 * miss will simply return <tt>null</tt>.</p>
 *
 * <p>If you do provide an allocator, and it throws a ClassCastException (because you tried to get a non-K type key),
 * that exception will be thrown.</p>
 *
 * <p>This class does not allow <tt>null</tt> keys or values. Further key/value restrictions depend on the type of
 * backing Map used; the default is HashMap.</p>
 *
 * <p>This class is not thread safe.</p>
 * @param <K>
 * @param <V>
 */
public class CacheMap<K,V> extends LinkedHashMap<K,V> {
    /**
     * Creates a value V for a key K, if needed.
     * @param <K> the key type
     * @param <V> the value type
     */
    public interface Allocator<K,V> {
        V allocateFor(K key);
    }

    private final int maxSize;
    private final Allocator<K,V> allocator;

    public CacheMap() {
        this(null);
    }

    public CacheMap(Allocator<K,V> allocator) {
        this(100, allocator);
    }

    public CacheMap(int size) {
        this(size, null);
    }

    public CacheMap(int size, Allocator<K,V> allocator) {
        super(size, .75f, true);
        ArgumentValidation.isGTE("size", size, 1);
        
        this.maxSize = size;
        this.allocator = allocator;
    }

    @Override
    public V get(Object key) {
        V ret = super.get(key);
        if ( (ret == null) && (allocator != null) ) {
            @SuppressWarnings("unchecked") K kKey = (K)key; // should throw ClassCastException if invalid type
            ret = allocator.allocateFor(kKey);
            allocatorHook();
            V shouldBeNull = put(kKey, ret);
            assert shouldBeNull == null : String.format("%s not null for put(%s,%s)", shouldBeNull, kKey, ret);
        }
        return ret;
    }

    protected void allocatorHook()
    {}

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }
}
