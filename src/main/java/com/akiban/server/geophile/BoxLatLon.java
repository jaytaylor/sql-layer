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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class BoxLatLon implements SpatialObject
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

    // BoxLatLon interface

    public BoxLatLon(BigDecimal latLo, BigDecimal latHi, BigDecimal lonLo, BigDecimal lonHi)
    {
        // SpaceLatLon.scale will truncate and fractional part. That's OK for latLo and lonLo. For latHi and lonHi,
        // we want to round up, to represent the fact that the box represented by the coarser (scaled) space
        // is not empty.
        this.latLo = SpaceLatLon.scaleLat(latLo);
        this.latHi = SpaceLatLon.scaleLat(latHi.round(ROUND_UP));
        this.lonLo = SpaceLatLon.scaleLon(lonLo);
        this.lonHi = SpaceLatLon.scaleLon(lonHi.round(ROUND_UP));
    }

    // Class state

    private static final MathContext ROUND_UP = new MathContext(0, RoundingMode.CEILING);

    // Object state

    private final long latLo;
    private final long latHi;
    private final long lonLo;
    private final long lonHi;
}
