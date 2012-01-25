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

public class InOutTap
{

    public void in() {
        internal.in();
    }

    public void out() {
        internal.out();
    }

    /**
     * Reset the tap.
     * @deprecated using this method indicates an improper separation of concerns between defining events (which
     * is what this class does) and reporting on them.
     */
    @Deprecated
    public void reset() {
        internal.reset();
    }

    /**
     * Gets the duration of the tap's in-to-out timer.
     * @deprecated using this method indicates an improper separation of concerns between defining events (which
     * is what this class does) and reporting on them.
     * @return the tap's duration
     */
    @Deprecated
    public long getDuration() {
        return internal.getDuration();
    }

    /**
     * Gets the tap's report
     * @deprecated using this method indicates an improper separation of concerns between defining events (which
     * is what this class does) and reporting on them.
     * @return the underlying tap's report
     */
    @Deprecated
    public TapReport getReport() {
        return internal.getReport();
    }

    InOutTap(Tap internal) {
        this.internal = internal;
    }

    private final Tap internal;
}
