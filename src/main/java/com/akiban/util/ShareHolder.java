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

import javax.xml.ws.Holder;

public final class ShareHolder<T extends Shareable> {

    // ShareHolder interface

    public T get() {
        return held;
    }

    public boolean hasItem() {
        return held != null;
    }

    public void hold(T item) {
        if (item == null) {
            throw new IllegalArgumentException("can't hold null elements");
        }
        if (held != null) {
            release();
        }
        item.share();
        held = item;
    }

    public T release() {
        held.release();
        T result = held;
        held = null;
        return result;
    }

    public void releaseIf() {
        if (held != null) release();
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
