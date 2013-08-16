/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util;

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
    
    public boolean isEmpty() {
        return definedElements.isEmpty();
    }

    public void clear() {
        Arrays.fill(internalArray, null);
        definedElements.clear();
    }

    public String describeElements() {
        StringBuilder sb = new StringBuilder();
        describeElements(sb);
        return sb.toString();
    }

    public StringBuilder describeElements(StringBuilder sb) {
        sb.append('[');
        int ilen = sb.length();
        
        int size = definedElements.size(); 
        for (int i = 0; i < size; ++i) {
            if (isDefined(i))
                sb.append(internalGet(i)).append(", ");
        }
        if (sb.length() > ilen)             // sb is not just the initial '['
            sb.setLength(sb.length() - 2);  // snip off the trailing ", "
        sb.append(']');

        return sb;
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
        internalArray = new Object[initialCapacity];
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
