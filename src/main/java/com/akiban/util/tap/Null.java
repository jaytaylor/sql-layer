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

package com.akiban.util.tap;

/**
 * A {@link com.akiban.util.tap.Tap} subclass that does nothing. This is the initial target of
 * every added {@link com.akiban.util.tap.Dispatch}.
 *
 */
class Null extends Tap {

    public Null(final String name) {
        super(name);
    }

    public void in() {
        // do nothing
    }

    public void out() {
        // do nothing
    }

    public long getDuration() {
        return 0;
    }

    public void reset() {
        // do nothing
    }

    public void appendReport(final StringBuilder sb) {
        // do nothing;
    }

    public String toString() {
        return "NullTap(" + name + ")";
    }

    public TapReport getReport() {
        return null;
    }
}
