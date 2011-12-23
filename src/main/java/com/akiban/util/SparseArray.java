/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.akiban.util;

import java.util.BitSet;

public abstract class SparseArray<T> {
    
    public T get(int index) {
        if (index < 0)
            throw new IndexOutOfBoundsException(Integer.toString(index));
        if (index > internalArray.length) {
            int newSize = internalArray.length * GROW_FACTOR;
            if (newSize < index)
                newSize = index + 1;
            Object[] newInternalArray = new Object[newSize];
            System.arraycopy(internalArray, 0, newInternalArray, 0, internalArray.length);
            internalArray = newInternalArray;
        }
        if (internalArray[index] == null) {
            internalArray[index] = initialValue();
        }
        definedElements.set(index);
        @SuppressWarnings("unchecked")
        T cast = (T) internalArray[index];
        return cast;
    }

    protected T initialValue() {
        return null;
    }

    @Override
    public String toString() {
        return "SparseArray(" + definedElements.cardinality() + " defined: " + definedElements + ')';
    }

    private Object[] internalArray = new Object[INITIAL_SIZE];
    private BitSet definedElements = new BitSet(INITIAL_SIZE);
    
    private static final int INITIAL_SIZE = 10;
    private static final int GROW_FACTOR = 2;
}
