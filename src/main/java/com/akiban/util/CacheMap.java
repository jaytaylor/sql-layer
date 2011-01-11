package com.akiban.util;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
public class CacheMap<K,V> implements Map<K,V> {
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
    private final Map<K,V> intern;
    private final ArrayDeque<K> lruList;

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
        this(size, allocator, new HashMap<K,V>(size, 1.0f));
    }

    public CacheMap(int size, Allocator<K,V> allocator, Map<K,V> backingMap) {
        ArgumentValidation.isGTE("size", size, 1);
        ArgumentValidation.notNull("backing map", backingMap);
        
        this.maxSize = size;
        this.allocator = allocator;
        this.intern = backingMap;
        this.lruList = new ArrayDeque<K>(size);
    }

    @Override
    public int size() {
        return intern.size();
    }

    @Override
    public boolean isEmpty() {
        return intern.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return intern.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return intern.containsValue(value);
    }

    @Override
    public V get(Object key) {
        V ret = intern.get(key);
        if ( (ret == null) && (allocator != null) ) {
            @SuppressWarnings("unchecked") K kKey = (K)key;
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
    public V put(K key, V value) {
        ArgumentValidation.notNull("CacheMap key", key);
        ArgumentValidation.notNull("CacheMap value", value);
        final V old = intern.put(key, value);
        lruList.addFirst(key);
        if (intern.size() > maxSize) {
            K removeMe = lruList.removeLast();
            V removed = intern.remove(removeMe);
            assert removed != null : removeMe;
        }

        return old;
    }

    @Override
    public V remove(Object key) {
        return intern.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K,? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        intern.clear();
    }

    @Override
    public Set<K> keySet() {
        return intern.keySet();
    }

    @Override
    public Collection<V> values() {
        return intern.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return intern.entrySet();
    }

    @Override
    public int hashCode() {
        return intern.hashCode();
    }

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
    @Override
    public boolean equals(Object obj) {
        return intern.equals(obj);
    }

    @Override
    public String toString() {
        return intern.toString();
    }
}
