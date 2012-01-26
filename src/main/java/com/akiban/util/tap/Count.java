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
 * A Tap subclass that counts calls to {@link #in()} and {@link #out()}.
 * Generally this is faster than {@link TimeAndCount} because the system
 * clock is not read.
 */
class Count extends Tap
{
    // Object interface

    public String toString()
    {
        return String.format("%s inCount=%,d outCount=%,d", name, inCount, outCount);
    }

    // Tap interface

    public void in()
    {
        checkNesting();
        inCount++;
    }

    public void out()
    {
        outCount++;
        checkNesting();

    }

    public long getDuration()
    {
        return 0;
    }

    public void reset()
    {
        inCount = 0;
        outCount = 0;
    }

    public void appendReport(StringBuilder sb)
    {
        sb.append(String.format("%20s inCount=%,10d outCount=%,10d", name, inCount, outCount));
    }

    public TapReport getReport()
    {
        return new TapReport(name, inCount, outCount, 0);
    }

    // Count interface

    public Count(String name)
    {
        super(name);
    }
}
