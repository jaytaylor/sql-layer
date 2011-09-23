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

public final class ShareHolder<T extends Shareable> {

    // ShareHolder interface

    public T get() {
        if (held == null)
            throw new IllegalStateException("nothing held");
        return held;
    }

    public void hold(T item) {
        if (item == null) {
            throw new IllegalArgumentException("can't hold null elements");
        }
        if (isHolding()) {
            release();
        }
        assert held == null : held;
        held = item;
        held.share();
    }

    public void release() {
        if (held != null) {
            held.release();
            held = null;
        }
    }

    public boolean isHolding() {
        return held != null && held.isShared();
    }

    // object interface

    @Override
    public String toString() {
        if (held != null)
            return "Holder(" + held + ')';
        return "Holder( empty )";
    }

    // object state

    private T held;
}
