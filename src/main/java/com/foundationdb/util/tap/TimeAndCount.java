/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.util.tap;

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
        justEnabled = false;
        checkNesting();
        inCount++;
        inNanos = System.nanoTime();
    }

    public void out()
    {
        if (justEnabled) {
            justEnabled = false;
        } else {
            outCount++;
            boolean nestingOK = checkNesting();
            justEnabled = false;
            long now = System.nanoTime();
            endNanos = now;
            if (nestingOK) {
                lastDuration = now - inNanos;
                cumulativeNanos += lastDuration;
            }
            // else: Usage of this tap is non-nested. checkNesting() reported on the problem. But skip
            // maintenance and use of lastDuration to try and keep reported values approximately right.
        }
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
        justEnabled = true;
    }

    public void appendReport(String label, StringBuilder buffer)
    {
        buffer.append(String.format("%s %20s inCount=%,10d outCount=%,10d time=%,12dms",
                                    label, name, inCount, outCount, cumulativeNanos / 1000000));
        if (outCount > 0) {
            buffer.append(String.format("  per=%,12dns  interval=%,12dns",
                                        cumulativeNanos / outCount, (endNanos - startNanos) / outCount));
        }
    }

    public TapReport[] getReports()
    {
        return new TapReport[]{new TapReport(getName(), inCount, outCount, cumulativeNanos)};
    }

    // TimeAndCount interface

    public TimeAndCount(String name)
    {
        super(name);
    }

    // Object state

    private volatile long cumulativeNanos;
    private volatile long inNanos;
    private volatile long startNanos = System.nanoTime();
    private volatile long endNanos;
    private volatile long lastDuration = Long.MIN_VALUE;
    private volatile boolean justEnabled = false;
}
