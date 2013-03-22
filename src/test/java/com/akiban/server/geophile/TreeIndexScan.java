
package com.akiban.server.geophile;

import com.akiban.server.geophile.Scan;
import com.akiban.server.geophile.Space;
import com.akiban.server.geophile.SpatialObject;

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
