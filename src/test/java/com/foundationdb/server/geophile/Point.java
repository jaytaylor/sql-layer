/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
