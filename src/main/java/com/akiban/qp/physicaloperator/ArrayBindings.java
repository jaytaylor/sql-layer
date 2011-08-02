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

package com.akiban.qp.physicaloperator;

import com.akiban.util.Undef;

import java.util.Arrays;

public final class ArrayBindings extends Bindings
{
    private Object[] bindings;

    // ArrayBindings interface

    // queryParameters is the number of parameters in the query. There may be additional bindings, in which case
    // set(int, Object) will cause the array to grow.
    public ArrayBindings(int queryParameters) {
        bindings = new Object[queryParameters];
        for (int i=0; i < queryParameters; ++i) {
            bindings[i] = Undef.only();
        }
    }

    @Override
    public String toString() {
        return "ArrayBindings" + Arrays.toString(bindings); 
    }

    // Bindings interface

    @Override
    public Object get(int index) {
        final Object value;
        try {
            value = bindings[index];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new BindingNotSetException(e, index);
        }
        if (Undef.isUndefined(value)) {
            throw new BindingNotSetException(index);
        }
        return value;
    }

    @Override
    public void set(int index, Object value)
    {
        if (index >= bindings.length) {
            Object[] newBindings = new Object[index + 1];
            System.arraycopy(bindings, 0, newBindings, 0, bindings.length);
            bindings = newBindings;
        }
        bindings[index] = value;
    }

}
