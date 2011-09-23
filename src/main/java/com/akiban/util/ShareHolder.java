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

public final class ShareHolder<T extends Shareable> implements Shareable {

    // ShareHolder interface

    public T get() {
        return held;
    }

    public void hold(T item) {
        reserve(item);
        share();
    }

    public void reserve(T item) {
        if (item == null) {
            throw new IllegalArgumentException("can't hold null elements");
        }
        if (isShared()) {
            throw new IllegalStateException("can't hold while another item is shared: " + held);
        }
        held = item;
    }

    // Shareable interface

    @Override
    public void share() {
        checkHeld();
        held.share();
    }

    @Override
    public boolean isShared() {
        return held != null && held.isShared();
    }

    public void release() {
        checkHeld();
        held.release();
    }

    // object interface

    @Override
    public String toString() {
        if (held != null)
            return "Holder(" + held + ')';
        return "Holder( empty )";
    }

    // private methods
    private void checkHeld() {
        if (held == null) throw new IllegalStateException("no item held");
    }

    // object state

    private T held;
}
