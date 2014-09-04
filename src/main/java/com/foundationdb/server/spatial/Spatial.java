package com.foundationdb.server.spatial;

import com.geophile.z.Space;
import com.geophile.z.spatialobject.d2.Point;

/*

The lat/lon coordinate system is

- latitude: -90 to +90
- longitude: -180 to 180 with wraparound

These coordinates are typically given in fixed-point columns, so the interface is in terms of BigDecimal.
z-values have 57 bits of precision, and I'm guessing that it is preferable to bias toward longitude. This means
that we want 29 bits of precision for longitude, 28 for latitude, and longitude should be split first. I.e.,
the interleave pattern is [lon, lat, lon, lat, ..., lon].

log2(10) = 3.32, so to get 29 bits of precision for lon, that would be 29/3.32 = 8.7 digits. Up to three digits
are before the decimal place, leaving 6 after. For lat: 28/3.32 = 8.4 digits, and there are "nearly" three digits
before the decimal place. So we'll scale both by 10**6.

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

    public static final int LAT_LON_DIMENSIONS = 2;
    public static final double MIN_LAT = -90;
    public static final double MAX_LAT = 90;
    public static final double MIN_LON = -180;
    public static final double MAX_LON = 180;
    private static final int LAT_BITS = 28;
    private static final int LON_BITS = 29;
}
