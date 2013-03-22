
package com.akiban.server.geophile;

import com.akiban.server.error.OutOfRangeException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static com.akiban.server.geophile.SpaceLatLon.*;

public abstract class BoxLatLon implements SpatialObject
{
    public static BoxLatLon newBox(BigDecimal latLoDecimal,
                                   BigDecimal latHiDecimal,
                                   BigDecimal lonLoDecimal,
                                   BigDecimal lonHiDecimal)
    {
        long latLo = fixLat(scaleLat(latLoDecimal));
        long latHi = fixLat(scaleLat(latHiDecimal.round(ROUND_UP)));
        long lonLo = fixLon(scaleLon(lonLoDecimal));
        long lonHi = fixLon(scaleLon(lonHiDecimal.round(ROUND_UP)));
        return
            lonLo <= lonHi
            ? new BoxLatLonWithoutWraparound(latLo, latHi, lonLo, lonHi)
            : new BoxLatLonWithWraparound(latLo, latHi, lonLo, lonHi);
            
    }

    // For use by this class

    // Query boxes are specified as center point += delta. This calculation can put us past min/max lon.
    // The delta is measured in degrees, so we should be off by no more than 180`. If more than that, then
    // later checking will detect the problem.
    private static long fixLon(long lon)
    {
        if (lon > MAX_LON_SCALED + CIRCLE || lon < MIN_LON_SCALED - CIRCLE) {
            throw new OutOfRangeException(String.format("longitude %s", lon));
        }
        if (lon < MIN_LON_SCALED) {
            lon += CIRCLE;
        } else if (lon > MAX_LON_SCALED) {
            lon -= CIRCLE;
        }
        return lon;
    }

    // Fix lat by truncating at +/-90
    private static long fixLat(long lat)
    {
        if (lat > MAX_LAT_SCALED + CIRCLE || lat < MIN_LAT_SCALED - CIRCLE) {
            throw new OutOfRangeException(String.format("latitude %s", lat));
        }
        if (lat > MAX_LAT_SCALED) {
            lat = MAX_LAT_SCALED;
        } else if (lat < MIN_LAT_SCALED) {
            lat = MIN_LAT_SCALED;
        }
        return lat;
    }

    // Class state

    private static final MathContext ROUND_UP = new MathContext(0, RoundingMode.CEILING);
}
