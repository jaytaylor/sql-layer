/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.server.error.OutOfRangeException;

import java.math.BigDecimal;

public class SpaceLatLon extends Space
{
    /**
     * Compute the z-value for the given coordinates. The length of the z-value is that of the maximum resolution
     * for this space.
     * @param x Coordinates of point to be shuffled.
     * @return A z-value.
     */
    public long shuffle(BigDecimal ... x)
    {
        assert x.length == 2;
        assert x[0] != null;
        assert x[1] != null;
        long latScaled = scaleLat(x[0]);
        long lonScaled = scaleLon(x[1]);
        if (latScaled < MIN_LAT_SCALED || latScaled > MAX_LAT_SCALED ||
            lonScaled < MIN_LON_SCALED || lonScaled > MAX_LON_SCALED) {
            throw new IllegalArgumentException(String.format("(%s, %s) is not a valid latitude/longitude", x[0], x[1]));
        }
        long[] xScaled = new long[]{latScaled, lonScaled};
        return shuffle(xScaled);
    }

    public static long scaleLat(BigDecimal lat)
    {
        return lat.scaleByPowerOfTen(LOG_SCALE).longValue();
    }

    public static long scaleLon(BigDecimal lon)
    {
        return lon.scaleByPowerOfTen(LOG_SCALE).longValue();
    }

    public static SpaceLatLon create()
    {
        long[] lo = new long[]{-90 * SCALE, -180 * SCALE};
        long[] hi = new long[]{90 * SCALE, 180 * SCALE};
        int[] interleave = new int[MAX_Z_BITS];
        int d = 1; // start with longitude
        for (int zBit = 0; zBit < MAX_Z_BITS; zBit++) {
            interleave[zBit] = d;
            d = 1 - d;
        }
        return new SpaceLatLon(lo, hi, interleave);
    }

    private SpaceLatLon(long[] lo, long[] hi, int[] interleave)
    {
        super(lo, hi, interleave);
        assert zBits == MAX_Z_BITS;
        assert xBits[0] == 28;
        assert xBits[1] == 29;
    }

    public static final int MAX_DECOMPOSITION_Z_VALUES = 4;
    private static final long SCALE = 1000L * 1000L; // 10^6 (see derivation above)
    private static final int LOG_SCALE = 6; // log10(scale)
    static final long MIN_LAT_SCALED = -90 * SCALE;
    static final long MAX_LAT_SCALED = 90 * SCALE;
    static final long MIN_LON_SCALED = -180 * SCALE;
    static final long MAX_LON_SCALED = 180 * SCALE;
    static final long CIRCLE = 360 * SCALE;
}
