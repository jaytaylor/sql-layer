
package com.akiban.server.geophile;

import static java.lang.Math.max;
import static java.lang.Math.min;

class BoxLatLonWithoutWraparound extends BoxLatLon
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s:%s, %s:%s)", latLo, latHi, lonLo, lonHi);
    }

    // SpatialObject interface

    public long[] arbitraryPoint()
    {
        return new long[]{latLo, lonLo};
    }
    public boolean containedBy(Region region)
    {
        long rLatLo = region.lo(0);
        long rLonLo = region.lo(1);
        long rLatHi = region.hi(0);
        long rLonHi = region.hi(1);
        return rLatLo <= latLo && latHi <= rLatHi && rLonLo <= lonLo && lonHi <= rLonHi;
    }

    public RegionComparison compare(Region region)
    {
        long rLatLo = region.lo(0);
        long rLonLo = region.lo(1);
        long rLatHi = region.hi(0);
        long rLonHi = region.hi(1);
        if (latLo <= rLatLo && rLatHi <= latHi && lonLo <= rLonLo && rLonHi <= lonHi) {
            return RegionComparison.INSIDE;
        } else if (rLatHi < latLo || rLatLo > latHi || rLonHi < lonLo || rLonLo > lonHi) {
            return RegionComparison.OUTSIDE;
        } else {
            return RegionComparison.OVERLAP;
        }
    }

    // BoxLatLonWithoutWraparound interface

    public BoxLatLonWithoutWraparound(long latLo, long latHi, long lonLo, long lonHi)
    {
        this.latLo = max(latLo, SpaceLatLon.MIN_LAT_SCALED);
        this.latHi = min(latHi, SpaceLatLon.MAX_LAT_SCALED);
        this.lonLo = lonLo;
        this.lonHi = lonHi;
        checkLon(lonLo);
        checkLon(lonHi);
    }

    // For use by this class

    private void checkLat(long lat)
    {
        if (lat < SpaceLatLon.MIN_LAT_SCALED || lat > SpaceLatLon.MAX_LAT_SCALED) {
            throw new IllegalArgumentException(toString());
        }
    }

    private void checkLon(long lon)
    {
        if (lon < SpaceLatLon.MIN_LON_SCALED || lon > SpaceLatLon.MAX_LON_SCALED) {
            throw new IllegalArgumentException(toString());
        }
    }

    // Object state

    final long latLo;
    final long latHi;
    final long lonLo;
    final long lonHi;
}
