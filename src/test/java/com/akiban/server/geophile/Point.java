
package com.akiban.server.geophile;

import java.util.Arrays;

public class Point implements SpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        boolean first = true;
        for (long a : x) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(a);
        }
        buffer.append(')');
        return buffer.toString();
    }

    // SpatialObject interface

    public long[] arbitraryPoint()
    {
        return x;
    }

    public boolean containedBy(Region region)
    {
        int dimensions = x.length;
        for (int d = 0; d < dimensions; d++) {
            long xd = x[d];
            if (xd < region.lo(d) || xd > region.hi(d)) {
                return false;
            }
        }
        return true;
    }

    public RegionComparison compare(Region region)
    {
        // Points should only be shuffled on insert, not indexed.
        throw new UnsupportedOperationException();
    }

    // Point interface

    public long x(int d)
    {
        return x[d];
    }

    public Point(long[] x)
    {
        this.x = Arrays.copyOf(x, x.length);
    }

    // Object state

    private final long[] x;
}
