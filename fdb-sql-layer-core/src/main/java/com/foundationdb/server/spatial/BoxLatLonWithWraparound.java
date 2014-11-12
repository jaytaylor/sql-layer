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

package com.foundationdb.server.spatial;

import com.geophile.z.Space;
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.Region;
import com.geophile.z.space.RegionComparison;

import java.nio.ByteBuffer;

class BoxLatLonWithWraparound implements SpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s:%s, %s:%s)", left.yLo(), left.yHi(), right.xLo(), left.xHi());
    }

    // SpatialObject interface

    @Override
    public double[] arbitraryPoint()
    {
        return left.arbitraryPoint();
    }

    @Override
    public int maxZ()
    {
        return left.maxZ();
    }

    @Override
    public boolean containedBy(Region region)
    {
        // Only the topmost region can contain a box with wraparound
        return region.level() == 0;
    }

    @Override
    public boolean containedBy(Space space)
    {
        return left.containedBy(space) && right.containedBy(space);
    }

    @Override
    public RegionComparison compare(Region region)
    {
        RegionComparison cL = left.compare(region);
        RegionComparison cR = right.compare(region);
        if (cL == RegionComparison.REGION_INSIDE_OBJECT ||
            cR == RegionComparison.REGION_INSIDE_OBJECT) {
            return RegionComparison.REGION_INSIDE_OBJECT;
        } else if (cL == RegionComparison.REGION_OUTSIDE_OBJECT &&
                   cR == RegionComparison.REGION_OUTSIDE_OBJECT) {
            return RegionComparison.REGION_OUTSIDE_OBJECT;
        } else if (cL == RegionComparison.REGION_INSIDE_OBJECT &&
                   cR == RegionComparison.REGION_INSIDE_OBJECT) {
            assert false : region; // Can't be inside two disjoint boxes!
            return null;
        } else {
            return RegionComparison.REGION_OVERLAPS_OBJECT;
        }
    }

    @Override
    public void readFrom(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    // BoxLatLonWithWraparound interface

    public BoxLatLonWithWraparound(double latLo, double latHi, double lonLo, double lonHi)
    {
        left = new Box(latLo, latHi, Spatial.MIN_LON, lonHi);
        right = new Box(latLo, latHi, lonLo, Spatial.MAX_LON);
    }

    // Object state

    private final Box left;
    private final Box right;
}
