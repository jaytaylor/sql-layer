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

/**
 * A Bindable that simply returns a value, ignoring the given Bindings.
 * @param <T> the value's type
 */
public final class UnbindableBindable<T> implements Bindable<T> {

    public static <T> Bindable<T> ofNull(@SuppressWarnings("ignored") Class<T> cls) {
        return new UnbindableBindable<T>(null);
    }

    public static <T> Bindable<T> of(T value) {
        return new UnbindableBindable<T>(value);
    }

    private T value;

    public UnbindableBindable(T value) {
        this.value = value;
    }

    @Override
    public T bindTo(Bindings bindings) {
        return value;
    }
}
