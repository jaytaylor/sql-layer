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

package com.akiban.server.mttests.mtutil;

public final class Timing {
    /**
     * Sleeps in a way that gets around code instrumentation which may end up shortening Thread.sleep.
     * If Thread.sleep throws an exception, this will invoke {@code Thread.currentThread().interrupt()} to propagate it.
     * @param millis how long to sleep
     */
    public static void sleep(long millis) {
        final long start = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (System.currentTimeMillis() - start < millis);
    }

}
