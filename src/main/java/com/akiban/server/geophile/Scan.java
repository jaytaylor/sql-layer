
package com.akiban.server.geophile;

public interface Scan
{
    SpatialObject next();
    void close();
}
