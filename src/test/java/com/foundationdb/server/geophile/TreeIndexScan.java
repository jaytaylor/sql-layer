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

import com.foundationdb.server.geophile.Scan;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.geophile.SpatialObject;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class TreeIndexScan implements Scan
{
    // Scan interface

    public SpatialObject next()
    {
        SpatialObject next = null;
        while (!done && (spatialObjectIterator == null || !spatialObjectIterator.hasNext())) {
            if (zIterator.hasNext()) {
                Map.Entry<Long, Set<SpatialObject>> entry = zIterator.next();
                long z = entry.getKey();
                if (space.contains(zStart, z)) {
                    Set<SpatialObject> spatialObjects = entry.getValue();
                    spatialObjectIterator = spatialObjects.iterator();
                } else {
                    done = true;
                }
            } else {
                done = true;
            }
        }
        if (spatialObjectIterator != null && spatialObjectIterator.hasNext()) {
            next = spatialObjectIterator.next();
        }
        return next;
    }

    public void close()
    {
    }

    // TreeIndexScan interface

    public TreeIndexScan(Space space,
                         long zStart,
                         Iterator<Map.Entry<Long, Set<SpatialObject>>> iterator)
    {
        this.space = space;
        this.zStart = zStart;
        this.zIterator = iterator;
        this.done = false;
    }

    // Object state

    private final Space space;
    private final long zStart;
    // Iterator over the map
    private final Iterator<Map.Entry<Long, Set<SpatialObject>>> zIterator;
    // Iterator over a map entry's Set<SpatialObject>
    private Iterator<SpatialObject> spatialObjectIterator;
    private boolean done;
}
