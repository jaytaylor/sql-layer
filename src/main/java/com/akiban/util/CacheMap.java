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
