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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

class Decomposer extends Space
{
    public void decompose(SpatialObject spatialObject, long[] zs)
    {
        int maxRegions = zs.length;
        int zCount = 0;
        long[] x = spatialObject.arbitraryPoint();
        Region region = new Region(this, x, x, zBits);
        // Find smallest region containing spatialObject.
        while (!spatialObject.containedBy(region)) {
            region.up();
        }
        // Split breadth-first, allowing up to maxElements elements.
        Queue<Region> queue = new ArrayDeque<Region>(maxRegions);
        queue.add(region);
        while (!queue.isEmpty()) {
            region = queue.poll();
            if (region.isPoint()) {
                zs[zCount++] = region.z();
            } else {
                region.downLeft();
                RegionComparison leftComparison = spatialObject.compare(region);
                region.up();
                region.downRight();
                RegionComparison rightComparison = spatialObject.compare(region);
                switch (leftComparison.concat(rightComparison)) {
                    case OUTSIDE_OUTSIDE:
                        assert false;
                        break;
                    case OUTSIDE_OVERLAP:
                        queue.add(region);
                        break;
                    case OUTSIDE_INSIDE:
                        zs[zCount++] = region.z();
                        break;
                    case OVERLAP_OUTSIDE:
                        region.up();
                        region.downLeft();
                        queue.add(region);
                        break;
                    case OVERLAP_OVERLAP:
                        if (queue.size() + 1 + zCount < maxRegions) {
                            queue.add(region.copy());
                            region.up();
                            region.downLeft();
                            queue.add(region);
                        } else {
                            region.up();
                            zs[zCount++] = region.z();
                        }
                        break;
                    case OVERLAP_INSIDE:
                        if (queue.size() + 1  + zCount < maxRegions) {
                            zs[zCount++] = region.z();
                            region.up();
                            region.downLeft();
                            queue.add(region);
                        } else {
                            region.up();
                            zs[zCount++] = region.z();
                        }
                        break;
                    case INSIDE_OUTSIDE:
                        region.up();
                        region.downLeft();
                        zs[zCount++] = region.z();
                        break;
                    case INSIDE_OVERLAP:
                        if (queue.size() + 1  + zCount < maxRegions) {
                            queue.add(region.copy());
                            region.up();
                            region.downLeft();
                            zs[zCount++] = region.z();
                        } else {
                            region.up();
                            zs[zCount++] = region.z();
                        }
                        break;
                    case INSIDE_INSIDE:
                        zs[zCount++] = region.z();
                        region.up();
                        region.downLeft();
                        zs[zCount++] = region.z();
                        break;
                }
            }
        }
        // Convert remaining elements in the queue
        while (!queue.isEmpty()) {
            region = queue.poll();
            zs[zCount++] = region.z();
        }
        // Fill unused with -1
        for (int i = zCount; i < maxRegions; i++) {
            zs[i] = -1L;
        }
        // Combine neighboring zs
        Arrays.sort(zs, 0, zCount);
        boolean merge;
        do {
            merge = false;
            for (int i = 1; i < zCount; i++) {
                long a = zs[i - 1];
                long b = zs[i];
                if (siblings(a, b)) {
                    zs[i - 1] = parent(a);
                    System.arraycopy(zs, i + 1, zs, i, zCount - (i + 1));
                    zs[--zCount] = -1L;
                    merge = true;
                }
            }
        } while (merge);
    }

    public Decomposer(Space space)
    {
        super(space);
    }
}
