
package com.akiban.server.geophile;

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
