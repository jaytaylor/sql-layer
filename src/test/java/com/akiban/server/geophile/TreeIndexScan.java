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
