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

import com.foundationdb.server.error.OutOfRangeException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static com.foundationdb.server.geophile.SpaceLatLon.*;

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
