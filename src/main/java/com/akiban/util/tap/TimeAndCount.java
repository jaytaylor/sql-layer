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
 * A Tap subclass that counts and times the intervals between calls to
 * {@link #in()} and {@link #out()}.
 */
class TimeAndCount extends Tap {

    public TimeAndCount(final String name) {
        super(name);
    }

    volatile long cumulativeNanos = 0;
    volatile long inCount = 0;
    volatile long outCount = 0;
    volatile long inNanos = Long.MIN_VALUE;
    volatile long startNanos = System.nanoTime();
    volatile long endNanos = System.nanoTime();
    volatile long lastDuration = Long.MIN_VALUE;

    public void in() {
        inCount++;
        inNanos = System.nanoTime();
    }

    public void out() {
        if (inNanos != Long.MIN_VALUE) {
            long now = System.nanoTime();
            endNanos = now;
            lastDuration = now - inNanos;
            cumulativeNanos += lastDuration;
            outCount++;
            inNanos = Long.MIN_VALUE;
        }
    }

    public long getDuration() {
        return lastDuration;
    }

    public void reset() {
        inCount = 0;
        outCount = 0;
        cumulativeNanos = 0;
        inNanos = Long.MIN_VALUE;
    }

    public void appendReport(final StringBuilder sb) {
        sb.append(String.format(
                "%20s inCount=%,10d outCount=%,10d time=%,12dms", name,
                inCount, outCount, cumulativeNanos / 1000000));
        if (outCount > 0) {
            sb.append(String.format("  per=%,12dns  interval=%,12dns",
                    cumulativeNanos / outCount, (endNanos - startNanos)
                            / outCount));
        }
    }

    public String toString() {
        return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                name, inCount, outCount, cumulativeNanos / 1000000);
    }

    public TapReport getReport() {
        return new TapReport(getName(), inCount, outCount, cumulativeNanos);
    }

}
