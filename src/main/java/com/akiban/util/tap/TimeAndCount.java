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
class TimeAndCount extends Tap
{
    // Object interface

    public String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d time=%,dms",
                             name, inCount, outCount, cumulativeNanos / 1000000);
    }

    // Tap interface

    public void in()
    {
        checkNesting();
        inCount++;
        inNanos = System.nanoTime();
    }

    public void out()
    {
        outCount++;
        boolean nestingOK = checkNesting();
        long now = System.nanoTime();
        endNanos = now;
        if (nestingOK) {
            lastDuration = now - inNanos;
            cumulativeNanos += lastDuration;
        }
        // else: Usage of this tap is non-nested. checkNesting() reported on the problem. But skip
        // maintenance and use of lastDuration to try and keep reported values approximately right.
    }

    public long getDuration()
    {
        return lastDuration;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
        cumulativeNanos = 0;
    }

    public void appendReport(StringBuilder sb)
    {
        sb.append(String.format("%20s inCount=%,10d outCount=%,10d time=%,12dms",
                                name, inCount, outCount, cumulativeNanos / 1000000));
        if (outCount > 0) {
            sb.append(String.format("  per=%,12dns  interval=%,12dns",
                                    cumulativeNanos / outCount, (endNanos - startNanos) / outCount));
        }
    }

    public TapReport getReport()
    {
        return new TapReport(getName(), inCount, outCount, cumulativeNanos);
    }

    // TimeAndCount interface

    public TimeAndCount(final String name)
    {
        super(name);
    }

    // Object state

    private volatile long cumulativeNanos = 0;
    private volatile long inNanos = Long.MIN_VALUE;
    private volatile long startNanos = System.nanoTime();
    private volatile long endNanos = System.nanoTime();
    private volatile long lastDuration = Long.MIN_VALUE;
}
