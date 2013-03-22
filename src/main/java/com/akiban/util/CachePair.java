
package com.akiban.util;

public final class CachePair<K,V> {

    // CachePair interface

    public V get(K key) {
        ArgumentValidation.notNull("key", key);
        synchronized (lock) {
            // There's probably a more efficient way to do this, with volatiles etc. Double-checked locking?
            if (this.key != key) {
                this.value = valueProvider.valueFor(key);
                this.key = key;
            }
            return value;
        }
    }

    @Override
    public String toString() {
        final K localKey;
        final V localValue;
        synchronized (lock) {
            localKey = key;
            localValue = value;
        }

        StringBuilder builder = new StringBuilder("CachedPair(");
        append(builder, localKey);
        builder.append(" -> ");
        append(builder, localValue);
        builder.append(')');
        return builder.toString();
    }

    // class interface

    public static <K,V> CachePair<K,V> using(CachedValueProvider<? super K, ? extends V> valueProvider) {
        return new CachePair<>(valueProvider);
    }

    // for use by this class

    private CachePair(CachedValueProvider<? super K, ? extends V> valueProvider) {
        this.valueProvider = valueProvider;
        ArgumentValidation.notNull("value provider", this.valueProvider);
    }

    private static void append(StringBuilder builder, Object obj) {
        if (obj == null) {
            builder.append("null");
        }
        else {
            builder.append("0x").append(Integer.toHexString(obj.hashCode())).append(' ').append(obj);
        }
    }

    // object state

    private final CachedValueProvider<? super K,? extends V> valueProvider;
    private final Object lock = new Object();
    private  K key = null;
    private V value = null;

    // nested classes

    public interface CachedValueProvider<K,V> {
        V valueFor(K key);
    }
}
