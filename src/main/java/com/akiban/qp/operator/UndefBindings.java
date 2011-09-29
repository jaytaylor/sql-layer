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

import com.akiban.util.Undef;

public final class UndefBindings extends Bindings {

    private static final UndefBindings INSTANCE = new UndefBindings();

    public static Bindings only() {
        return INSTANCE;
    }

    @Override
    public Object get(int index) {
        return Undef.only();
    }

    @Override
    public void set(int index, Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "UndefinedBindings";
    }
}
