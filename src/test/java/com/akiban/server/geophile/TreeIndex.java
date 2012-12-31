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
            spatialObjects = new HashSet<SpatialObject>();
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
        new TreeMap<Long, Set<SpatialObject>>();
}
