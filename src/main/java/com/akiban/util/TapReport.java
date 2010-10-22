package com.akiban.util;

import com.akiban.util.Tap.TimeAndCount;

import java.beans.ConstructorProperties;

/**
 * A structure to return {@link TimeAndCount} results. May be extended for use
 * by custom {@link Tap} subclasses.
 * 
 * @author peter
 * 
 */
public class TapReport {

    private final String name;

    private final long inCount;
    private final long outCount;

    private final long cumulativeTime;

    @ConstructorProperties( { "name", "inCount", "outCount", "cumulativeTime" })
    public TapReport(final String name, final long inCount,
            final long outCount, final long cumulativeTime) {
        this.name = name;
        this.inCount = inCount;
        this.outCount = outCount;
        this.cumulativeTime = cumulativeTime;
    }

    public String getName() {
        return name;
    }

    public long getInCount() {
        return inCount;
    }

    public long getOutCount() {
        return outCount;
    }

    public long getCumulativeTime() {
        return cumulativeTime;
    }
}