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

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class SparseArray<T> {

    /**
     * Returns the greatest index for which {@link #isDefined(int)} would return <tt>true</tt>
     * @return the logical "size" of this sparse array
     */
    public int lastDefinedIndex() {
        return definedElements.length();
    }

    /**
     * Returns whether this array could be expressed as a compact array. That is, whether all indexes
     * less than {@link #lastDefinedIndex()} would return <tt>true</tt> for {@link #isDefined(int)}. If this method
     * returns <tt>true</tt>, it is safe to call {@link #toList()}
     * @return
     */
    public boolean isCompactable() {
        return definedElements.length() == definedElements.cardinality();
    }

    @SuppressWarnings("unchecked") // T[] is just erased to Object[] anyway, which is what we have
    public List<T> toList() {
        if (!isCompactable())
            throw new IllegalArgumentException("Not compactable");
        T[] arrayCopy = Arrays.copyOf((T[])internalArray, definedElements.length());
        return Arrays.asList(arrayCopy);
    }

    /**
     * Gets the element at the specified index. If that element had not been previously defined, its initial
     * value will be taken from {@link #initialValue()}. When this method returns, the element at the index
     * will always be defined.
     * @param index the index to retrieve
     * @return the element at the specified index
     */
    public T get(int index) {
        if (index < 0)
            throw new IndexOutOfBoundsException(Integer.toString(index));
        if (!definedElements.get(index)) {
            ensureCapacity(index);
            internalArray[index] = initialValue();
            definedElements.set(index);
        }
        return internalGet(index);
    }
    
    public T getIfDefined(int index) {
        if (index < 0)
            throw new IndexOutOfBoundsException(Integer.toString(index));
        if (!definedElements.get(index))
            throw new IllegalArgumentException("undefined value at index " + index);
        return internalGet(index);
    }

    /**
     * Sets the element at the specified index. This also marks that index as defined. This method returns the old
     * value at the index, or {@code null} if it was undefined. This means that if defined methods can ever be null
     * in your usage (either because of sets or initial values), you cannot use this method to determine whether the
     * old value had been defined. Use {@link #isDefined(int)} instead.
     * @param index the index to set
     * @param item the new value
     * @return the old element at this index, or {@code null} if it was undefined
     */
    public T set(int index, T item) {
        if (index < 0)
            throw new IndexOutOfBoundsException(Integer.toString(index));
        ensureCapacity(index);
        T old = internalGet(index);
        // if old != null, this element was definitely defined before, so don't bother redefining it.
        // if old == null, this element may or may not have been defined, but redefining it is idempotent
        if (old == null)
            definedElements.set(index);
        internalArray[index] = item;
        return old;
    }
    
    public boolean isDefined(int index) {
        return definedElements.get(index);
    }
    
    public String describeElements() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        
        int size = definedElements.size(); 
        for (int i = 0; i < size; ++i) {
            if (isDefined(i))
                sb.append(internalGet(i)).append(", ");
        }
        if (sb.length() > 1)                // sb is not just the initial '['
            sb.setLength(sb.length() - 2);  // snip off the trailing ", "
        sb.append(']');

        return sb.toString();
    }

    protected T initialValue() {
        return null;
    }

    @Override
    public String toString() {
        return "SparseArray(" + definedElements.cardinality() + " defined: " + definedElements + ')';
    }
    
    // intended for testing
    int currentCapacity() {
        return internalArray.length;
    }

    private void ensureCapacity(int index) {
        if (internalArray.length <= index) {
            int newSize = internalArray.length * GROW_FACTOR;
            if (newSize <= index)
                newSize = index + 1;
            Object[] newInternalArray = new Object[newSize];
            System.arraycopy(internalArray, 0, newInternalArray, 0, internalArray.length);
            internalArray = newInternalArray;
        }
    }
    
    @SuppressWarnings("unchecked")
    private T internalGet(int index) {
        return (T) internalArray[index];
    }

    public SparseArray(int initialCapacity) {
        internalArray = new Object[initialCapacity+1];
        definedElements = new BitSet(initialCapacity);
    }
    
    public SparseArray() {
        this(INITIAL_SIZE);
    }
    
    private Object[] internalArray;
    private BitSet definedElements;
    
    private static final int INITIAL_SIZE = 10;
    private static final int GROW_FACTOR = 2;
}
