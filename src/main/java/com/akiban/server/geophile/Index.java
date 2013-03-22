
package com.akiban.server.geophile;

public interface Index
{
    boolean add(long z, SpatialObject spatialObject);
    boolean remove(long z, SpatialObject spatialObject);
    Scan scan(long z);
}
