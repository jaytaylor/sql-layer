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

import java.beans.ConstructorProperties;

public class TapReport
{
    @ConstructorProperties({"name", "inCount", "outCount", "cumulativeTime"})
    public TapReport(String name, long inCount, long outCount, long cumulativeTime)
    {
        this.name = name;
        this.inCount = inCount;
        this.outCount = outCount;
        this.cumulativeTime = cumulativeTime;
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

    // Object state

    private final String name;
    private final long inCount;
    private final long outCount;
    private final long cumulativeTime;
}