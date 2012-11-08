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

package com.akiban.server.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>Wrapper around a standard {@link Map} that provides thread safety through the usage of read and write locks.
 *
 * <p>All methods that read from the map first acquire a shared claim and all methods that write to the map first
 * acquire an exclusive claim. This allows many concurrent readers and exactly one writer. This is simple and sufficient
 * for read-mostly workloads.</p>
 *
 * <p>In addition to the Map interface, methods are provided for manually acquiring shared or exclusive, see
 * {@link #claimShared()} and {@link #claimExclusive()}, are available. These are useful when something with the
 * underlying map (e.g cleanup) needs to happen in a globally consistent way.
 */
public class ReadWriteMap<K,V> implements Map<K,V> {

    public interface ValueCreator<K,V> {
        V createValueForKey(K key);
    }


    private final Map<K,V> map;
    private final ReadWriteLock rwLock;
    private final Lock shared;
    private final Lock exclusive;

    private ReadWriteMap(Map<K,V> map, boolean isFair) {
        this.map = map;
        this.rwLock = new ReentrantReadWriteLock(isFair);
        this.shared = rwLock.readLock();
        this.exclusive = rwLock.writeLock();
    }

    public static <K,V> ReadWriteMap<K,V> wrapNonFair(Map<K,V> map) {
        return new ReadWriteMap<K,V>(map, false);
    }

    public static <K,V> ReadWriteMap<K,V> wrapFair(Map<K,V> map) {
        return new ReadWriteMap<K,V>(map, true);
    }


    //
    // Manual lock management
    //

    /** Claim shared access for an extended period. Must be paired (i.e. try/finally) with {@link #releaseShared()} */
    public void claimShared() {
        shared.lock();
    }

    /** Release previously acquired shared access. */
    public void releaseShared() {
        shared.unlock();
    }

    /** Claim exclusive access for an extended period. Must be paired (i.e. try/finally) with {@link #releaseExclusive()} */
    public void claimExclusive() {
        exclusive.lock();
    }

    /** Release previously acquired exclusive access. */
    public void releaseExclusive() {
        exclusive.unlock();
    }

    /**
     * Return the map this wrapper was constructed with. Note that using this without first calling
     * {@link #claimShared()} or {@link #claimExclusive()} removes guarantees provided by this interface.
     */
    public Map<K,V> getWrappedMap() {
        return map;
    }

    /**
     * Check that the map does not contain the given key, throwing an {@link IllegalStateException} if it does, and then
     * delegate to {@link #put(Object, Object)}.
     */
    public V putNewKey(K key, V value) {
        claimExclusive();
        try {
            if(map.containsKey(key)) {
                throw new IllegalStateException("Expected new key: " + key);
            }
            return map.put(key, value);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * <p>Attempt to get a value by first delegating to {@link #get(Object)}. If the key does not exist, create a new
     * default value and put it in the map.</p>
     *
     * <p>The creation and putting of a new default value is performed under a single exclusive claim such that any
     * any concurrent callers of this method will only cause 1 value to be created.</p>
     */
    public V getOrCreateAndPut(K key, ValueCreator<K,V> defaultValueCreator) {
        V value = get(key);
        if(value == null) {
            claimExclusive();
            try {
                // Check again under exclusive lock
                value = map.get(key);
                if(value == null) {
                    value = defaultValueCreator.createValueForKey(key);
                    map.put(key, value);
                }
            } finally {
                releaseExclusive();
            }
        }
        return value;
    }

    /**
     * Update a key to a new value if and only if the current value is as given.
     * @return true if the key was updated to the new value
     */
    public boolean compareAndSet(K key, V expected, V update) {
        claimExclusive();
        try {
            V current = map.get(key);
            if(expected != current) {
                return false;
            }
            map.put(key, update);
            return true;
        } finally {
            releaseExclusive();
        }
    }


    //
    // Map interface
    //

    @Override
    public int size() {
        claimShared();
        try {
            return map.size();
        } finally {
            releaseShared();
        }
    }

    @Override
    public boolean isEmpty() {
        claimShared();
        try {
            return map.isEmpty();
        } finally {
            releaseShared();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        claimShared();
        try {
            return map.containsKey(key);
        } finally {
            releaseShared();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        claimShared();
        try {
            return map.containsValue(value);
        } finally {
            releaseShared();
        }
    }

    @Override
    public V get(Object key) {
        claimShared();
        try {
            return map.get(key);
        } finally {
            releaseShared();
        }
    }

    @Override
    public V put(K key, V value) {
        claimExclusive();
        try {
            return map.put(key, value);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public V remove(Object key) {
        claimExclusive();
        try {
            return map.remove(key);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public void putAll(Map<? extends K,? extends V> m) {
        claimExclusive();
        try {
            map.putAll(m);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public void clear() {
        claimExclusive();
        try {
            map.clear();
        } finally {
            releaseExclusive();
        }
    }

    private static final String UNSUPPORTED_MSG = "Unsupported. Call getWrappedMap after reading JavaDoc.";

    /** <b>Unsupported</b>. Use {@link #claimShared()} or {@link #claimExclusive()} and then {@link #getWrappedMap()} */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** <b>Unsupported</b>. Use {@link #claimShared()} or {@link #claimExclusive()} and then {@link #getWrappedMap()} */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** <b>Unsupported</b>. Use {@link #claimShared()} or {@link #claimExclusive()} and then {@link #getWrappedMap()} */
    @Override
    public Set<Entry<K,V>> entrySet() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public String toString() {
        claimShared();
        try {
            return map.toString();
        } finally {
            releaseShared();
        }
    }
}
