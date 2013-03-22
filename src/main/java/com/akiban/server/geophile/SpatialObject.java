
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
