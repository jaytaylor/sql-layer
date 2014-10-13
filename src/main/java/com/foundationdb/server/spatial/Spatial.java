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

package com.foundationdb.server.spatial;

import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import com.geophile.z.spatialobject.jts.JTSBase;

import java.nio.ByteBuffer;

/*

The lat/lon coordinate system is

- latitude: -90.0 to +90.0
- longitude: -180.0 to 180.0 with wraparound

The interleave pattern is [lon, lat, lon, lat, ..., lon], reflecting the fact that longitude covers a numeric range
twice that of latitude.

 */

public class Spatial
{
    public static Space createLatLonSpace()
    {
        int[] interleave = new int[LAT_BITS + LON_BITS];
        int dimension = 1; // Start with lon, as described above
        for (int d = 0; d < LAT_BITS + LON_BITS; d++) {
            interleave[d] = dimension;
            dimension = 1 - dimension;
        }
        return Space.newSpace(new double[]{MIN_LAT, MIN_LON},
                              new double[]{MAX_LAT, MAX_LON},
                              new int[]{LAT_BITS, LON_BITS},
                              interleave);
    }

    public static long shuffle(Space space, double x, double y)
    {
        Point point = new Point(x, y);
        long[] zValues = new long[1];
        space.decompose(point, zValues);
        long z = zValues[0];
        assert z != Space.Z_NULL;
        return z;
    }

    public static void shuffle(Space space, SpatialObject spatialObject, long[] zs)
    {
        space.decompose(spatialObject, zs);
    }

    public static byte[] serialize(JTSBase spatialObject)
    {
        ByteBuffer buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        boolean serialized = false;
        while (!serialized) {
            try {
                spatialObject.writeTo(buffer);
                serialized = true;
            } catch (Exception e) {
                buffer = ByteBuffer.allocate(buffer.capacity() * 2);
            }
        }
        return buffer.array();
    }

    public static final int LAT_LON_DIMENSIONS = 2;
    public static final double MIN_LAT = -90;
    public static final double MAX_LAT = 90;
    public static final double MIN_LON = -180;
    public static final double MAX_LON = 180;
    private static final int LAT_BITS = 28;
    private static final int LON_BITS = 29;
    private static final int INITIAL_BUFFER_SIZE = 50;

    public enum SpatialObjectEncoding
    {
        WKB(1);

        public int encodingId()
        {
            return encodingId;
        }

        private SpatialObjectEncoding(int encodingId)
        {
            this.encodingId = encodingId;
        }

        private final int encodingId;
    }
}
