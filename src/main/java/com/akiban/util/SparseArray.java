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
        @SuppressWarnings("unchecked")
        T cast = (T) internalArray[index];
        return cast;
    }

    protected T initialValue() {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SparseArray(curr_capacity=");
        sb.append(internalArray.length).append(", defined at [");
        boolean atLeastOne = false;
        for (int i = 0; i < internalArray.length; ++i) {
            if (internalArray[i] != null) {
                atLeastOne = true;
                sb.append(i).append(", ");
            }
        }
        if (atLeastOne) {
            sb.setLength(sb.length() - 2); // snip off the trailing ", "
        }
        sb.append("] )");
        return sb.toString();
    }

    private Object[] internalArray = new Object[INITIAL_SIZE];
    
    private static final int INITIAL_SIZE = 10;
    private static final int GROW_FACTOR = 2;
}
