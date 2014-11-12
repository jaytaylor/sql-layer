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

import java.beans.ConstructorProperties;

public class TapReport
{
    @Override
    public String toString()
    {
        return String.format("%s: in = %s, out = %s, msec = %s", name, inCount, outCount, cumulativeTime / MILLION);
    }

    @ConstructorProperties({"name", "inCount", "outCount", "cumulativeTime"})
    public TapReport(String name, long inCount, long outCount, long cumulativeTime)
    {
        this.name = name;
        this.inCount = inCount;
        this.outCount = outCount;
        this.cumulativeTime = cumulativeTime;
    }
    
    public TapReport(String name)
    {
        this.name = name;
        this.inCount = 0;
        this.outCount = 0;
        this.cumulativeTime = 0;
    }

    public String getName()
    {
        return name;
    }

    public long getInCount()
    {
        return inCount;
    }

    public long getOutCount()
    {
        return outCount;
    }

    public long getCumulativeTime()
    {
        return cumulativeTime;
    }

    // Class state
    
    private static final int MILLION = 1000000;
    
    // Object state

    String name;
    long inCount;
    long outCount;
    long cumulativeTime;
}
