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

package com.foundationdb.server.geophile;

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
        Queue<Region> queue = new ArrayDeque<>(maxRegions);
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
                RegionComparison comparison = leftComparison.concat(rightComparison);
                switch (comparison) {
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
