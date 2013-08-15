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

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class TreeIndex implements Index
{
    // Index interface

    public boolean add(long z, SpatialObject spatialObject)
    {
        Set<SpatialObject> spatialObjects = map.get(z);
        if (spatialObjects == null) {
            spatialObjects = new HashSet<>();
            map.put(z, spatialObjects);
        }
        return spatialObjects.add(spatialObject);
    }

    public boolean remove(long z, SpatialObject spatialObject)
    {
        boolean removed = false;
        Set<SpatialObject> spatialObjects = map.get(z);
        if (spatialObjects != null && spatialObjects.remove(spatialObject)) {
            removed = true;
            if (spatialObjects.isEmpty()) {
                map.remove(z);
            }
        }
        return removed;
    }

    public Scan scan(long z)
    {
        return new TreeIndexScan(space, z, map.tailMap(z).entrySet().iterator());
    }

    // TreeIndex interface

    public TreeIndex(Space space)
    {
        this.space = space;
    }

    // Object state

    private final Space space;
    private final SortedMap<Long, Set<SpatialObject>> map =
        new TreeMap<>();
}
