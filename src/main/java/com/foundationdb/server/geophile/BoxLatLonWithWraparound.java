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

package com.foundationdb.server.geophile;

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
