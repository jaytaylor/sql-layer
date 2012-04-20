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
        return new CachePair<K, V>(valueProvider);
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
