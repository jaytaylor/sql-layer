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
