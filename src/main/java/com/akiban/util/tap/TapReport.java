
package com.akiban.util.tap;

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