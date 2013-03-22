
package com.akiban.server.geophile;

import com.akiban.server.geophile.Region;
import com.akiban.server.geophile.RegionComparison;
import com.akiban.server.geophile.SpatialObject;

public class Box2 implements SpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s:%s, %s:%s)", xLo, xHi, yLo, yHi);
    }

    // SpatialObject interface

    public long[] arbitraryPoint()
    {
        return new long[]{xLo, yLo};
    }

    public boolean containedBy(Region region)
    {
        long rXLo = region.lo(0);
        long rYLo = region.lo(1);
        long rXHi = region.hi(0);
        long rYHi = region.hi(1);
        return rXLo <= xLo && xHi <= rXHi && rYLo <= yLo && yHi <= rYHi;
    }

    public RegionComparison compare(Region region)
    {
        long rXLo = region.lo(0);
        long rYLo = region.lo(1);
        long rXHi = region.hi(0);
        long rYHi = region.hi(1);
        if (xLo <= rXLo && rXHi <= xHi && yLo <= rYLo && rYHi <= yHi) {
            return RegionComparison.INSIDE;
        } else if (rXHi < xLo || rXLo > xHi || rYHi < yLo || rYLo > yHi) {
            return RegionComparison.OUTSIDE;
        } else {
            return RegionComparison.OVERLAP;
        }
    }

    // Box2 interface

    public Box2(long xLo, long xHi, long yLo, long yHi)
    {
        this.xLo = xLo;
        this.xHi = xHi;
        this.yLo = yLo;
        this.yHi = yHi;
    }

    // Object state

    private final long xLo;
    private final long xHi;
    private final long yLo;
    private final long yHi;
}
