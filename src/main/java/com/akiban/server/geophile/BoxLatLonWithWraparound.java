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

class BoxLatLonWithWraparound extends BoxLatLon
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s:%s, %s:%s)", left.latLo, left.latHi, right.lonLo, left.lonHi);
    }

    // SpatialObject interface

    @Override
    public long[] arbitraryPoint()
    {
        return left.arbitraryPoint();
    }

    public boolean containedBy(Region region)
    {
        // Only the topmost region can contain a box with wraparound
        return region.level() == 0;
    }

    public RegionComparison compare(Region region)
    {
        RegionComparison cL = left.compare(region);
        RegionComparison cR = right.compare(region);
        switch (cL.concat(cR)) {
            case INSIDE_OUTSIDE:
            case OUTSIDE_INSIDE:
                return RegionComparison.INSIDE;
            case OUTSIDE_OUTSIDE:
                return RegionComparison.OUTSIDE;
            case INSIDE_INSIDE:
                assert false : region; // Can't be inside two disjoint boxes!
                return null;
            default:
                return RegionComparison.OVERLAP;
        }
    }

    // BoxLatLonWithWraparound interface

    public BoxLatLonWithWraparound(long latLo, long latHi, long lonLo, long lonHi)
    {
        left = new BoxLatLonWithoutWraparound(latLo, latHi, SpaceLatLon.MIN_LON_SCALED, lonHi);
        right = new BoxLatLonWithoutWraparound(latLo, latHi, lonLo, SpaceLatLon.MAX_LON_SCALED);
    }

    // Object state

    private final BoxLatLonWithoutWraparound left;
    private final BoxLatLonWithoutWraparound right;
}
