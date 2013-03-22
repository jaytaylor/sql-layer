/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.util;

import java.util.AbstractList;

/**
 * Utility class for seeing stack traces. Basically a thin shim around <tt>Thread.currentThread().getStackTrace()</tt>
 * that looks like a {@code List&lt;StackTraceElement&gt;}. Put one of these as a field in a class, and you'll be able
 * to see where its instances come from.
 */
public final class StackTracer extends AbstractList<StackTraceElement> {

    public StackTracer() {
        this.trace = Thread.currentThread().getStackTrace();
    }

    // AbstractList methods

    @Override
    public StackTraceElement get(int index) {
        return trace[index + TRIM];
    }

    @Override
    public int size() {
        return trace.length - TRIM;
    }

    private final StackTraceElement[] trace;
    private static final int TRIM = 2; // trim off the top two of the stack, which are getStackTrace and constructor
}
