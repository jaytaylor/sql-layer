/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
