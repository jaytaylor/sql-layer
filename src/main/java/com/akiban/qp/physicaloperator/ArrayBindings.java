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

public final class ArrayBindings implements Bindings {

    private final Object[] bindings;

    public ArrayBindings(int count) {
        bindings = new Object[count];
        for (int i=0; i < count; ++i) {
            bindings[i] = Undef.only();
        }
    }

    public void set(int index, Object value) {
        bindings[index] = value;
    }

    @Override
    public Object get(int index) {
        final Object value;
        try {
            value = bindings[index];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new BindingNotSetException(e);
        }
        if (Undef.isUndefined(value)) {
            throw new BindingNotSetException("binding not set at index " + index);
        }
        return value;
    }
}
