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

package com.akiban.server.geophile;

public interface SpatialObject
{
    /**
     * Returns the coordinates of a point inside this spatial object.
     * @return The coordinates of a point inside this spatial object.
     */
    long[] arbitraryPoint();

    /**
     * Indicates whether this spatial object is contained by the given region.
     * @param region The region to compare to.
     * @return true if this spatial object is contained by the region, false otherwise.
     */
    boolean containedBy(Region region);

    /**
     * Determine relationship of this spatial object to the given Region.
     * @param region region to compare.
     * @return RegionComparison
     */
    RegionComparison compare(Region region);
}
