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

package com.akiban.qp.operator;

import com.akiban.util.SparseArray;

public final class ArrayBindings extends Bindings
{
    private SparseArray<Object> bindings;

    // ArrayBindings interface

    // queryParameters is the number of parameters in the query. There may be additional bindings, in which case
    // set(int, Object) will cause the array to grow.
    public ArrayBindings(int queryParameters) {
        bindings = new SparseArray<Object>(queryParameters);
    }

    @Override
    public String toString() {
        return "ArrayBindings[ " + bindings.describeElements() + ']'; 
    }

    // Bindings interface

    @Override
    public Object get(int index) {
        if (!bindings.isDefined(index))
            throw new BindingNotSetException(index);
        return bindings.get(index);
    }

    @Override
    public void set(int index, Object value)
    {
        bindings.set(index, value);
    }

}
