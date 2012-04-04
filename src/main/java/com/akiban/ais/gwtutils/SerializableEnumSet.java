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

package com.akiban.ais.gwtutils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>A version of EnumSet that can be serialized over GWT.</p>
 *
 * <p>This uses ints for its serialization, and therefore has limitation that  you can't have more than {@value #MAX}
 * values in the enum definition).</p>
 *
 * @param <T> the enum to create a set for
 */
public final class SerializableEnumSet<T extends Enum<T>> implements Set<T>, Serializable
{
    /*
    Implementation notes:

    GWT cant' serialize EnumSet, so we need to declare it transient and, if we ever catch it being null,
    deserialize it from the "serialized" var. To do that, we need T's class, which GWT also can't serialize.
    So instead, we capture any ol' T (specifically, the first one), and use its class for creating EnumSet during
    deserialization.
     */
    public final static int MAX = 31;
    
    private int serialized;
    private T first;
    private transient EnumSet<T> set;

    /**
     * Creates an empty enum set.
     * @param cls the enum class to create a set for
     * @throws IllegalArgumentException if this class is not serializable or doesn't have any values
     */
    public SerializableEnumSet(Class<T> cls)
    {
        this.set = EnumSet.allOf(cls);
        if (set.size() > MAX)
        {
            throw new IllegalArgumentException("enum has too many values: " + set.size() + set);
        }
        if (set.size() == 0)
        {
            throw new IllegalArgumentException("enum has no values: " + cls);
        }
        this.first = (T) set.toArray()[0];
        set.clear();
        this.serialized = 0;
    }

    public int toInt()
    {
        init();
        return SerializableEnumSet.serializeEnumSet(set);
    }

    public void loadInt(int serialized)
    {
        this.serialized = serialized;
        set = null; // will force the next usage to re-initialize
    }

    private void init()
    {
        if (set == null)
        {
            @SuppressWarnings("unchecked") Class<T> cls = (Class<T>) first.getClass();
            set = deserializeEnumSet(cls, serialized);
        }
    }

    @SuppressWarnings("unused") // GWT
    private SerializableEnumSet()
    {
    }

    @Override
    public int size()
    {
        init();
        return set.size();
    }

    @Override
    public boolean isEmpty()
    {
        init();
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o)
    {
        init();
        return set.contains(o);
    }

    @Override
    public Iterator<T> iterator()
    {
        init();
        final Iterator<T> iter = set.iterator();
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }
            @Override
            public T next()
            {
                return iter.next();
            }
            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Object[] toArray()
    {
        init();
        return set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        init();
        return set.toArray(a);
    }

    @Override
    public boolean add(T t)
    {
        init();
        boolean ret = set.add(t);
        serialized = serializeEnumSet(set);
        return ret;
    }

    @Override
    public boolean remove(Object o)
    {
        init();
        boolean ret = set.remove(o);
        serialized = serializeEnumSet(set);
        return ret;
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        init();
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c)
    {
        init();
        boolean ret = set.addAll(c);
        serialized = serializeEnumSet(set);
        return ret;
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        init();
        boolean ret = set.retainAll(c);
        serialized = serializeEnumSet(set);
        return ret;
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        init();
        boolean ret = set.removeAll(c);
        serialized = serializeEnumSet(set);
        return ret;
    }

    @Override
    public void clear()
    {
        init();
        set.clear();
        serialized = 0;
        init();
    }

    /**
     * Serializes this set to an int
     * @param set the set to serialize
     * @return an int representation of it
     * @throws IllegalArgumentException if the set is non-empty <em>and</em> the contained enum has more than 31 values
     * @throws NullPointerException if set is null
     */
    private static int serializeEnumSet(EnumSet<?> set)
    {
        if (set.size() == 0) {
            return 0;
        }
        /* test size */
        {
            Enum<?> eItem = (Enum<?>) set.toArray()[0];
            if (EnumSet.allOf(eItem.getClass()).size() > MAX) {
                throw new IllegalArgumentException("too many items in enum type: " + eItem.getClass());
            }
        }
        int ret = 0;
        for (Enum<?> item : set) {
            ret += (1 << item.ordinal());
        }
        return ret;
    }

    /**
     * De-serializes an int to a set
     * @param cls the class of enums to de-serialize to
     * @param serialized the serialized form, as obtained from {@linkplain #serializeEnumSet(java.util.EnumSet)}
     * @param <T> an enum
     * @return the set
     * @throws NullPointerException if cls is null
     * @throws IllegalArgumentException if there are too many values in the enum type
     */
    private static <T extends Enum<T>> EnumSet<T> deserializeEnumSet(Class<T> cls, int serialized)
    {
        final Object[] allEnums;
        {
            EnumSet<T> all = EnumSet.allOf(cls);
            allEnums = all.toArray();
        }
        if (allEnums.length > MAX)
        {
            throw new IllegalArgumentException("too many enum values: (" + allEnums.length + ") "
                    + Arrays.asList(allEnums));
        }

        EnumSet<T> ret = EnumSet.noneOf(cls);
        for(int i=0; serialized != 0; serialized >>= 1, ++i)
        {
            if ( (serialized & 1) == 1 )
            {
                @SuppressWarnings("unchecked") T toAdd = (T) allEnums[i];
                ret.add(toAdd);
            }
        }
        return ret;
    }
}
