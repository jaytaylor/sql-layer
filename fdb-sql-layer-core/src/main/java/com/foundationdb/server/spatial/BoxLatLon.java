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

import com.foundationdb.server.error.OutOfRangeException;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Box;

public abstract class BoxLatLon
{
    public static SpatialObject newBox(double latLo,
                                       double latHi,
                                       double lonLo,
                                       double lonHi)
    {
        latLo = fixLat(latLo);
        latHi = fixLat(latHi);
        lonLo = fixLon(lonLo);
        lonHi = fixLon(lonHi);
        try {
            return
                lonLo <= lonHi
                ? new Box(latLo, latHi, lonLo, lonHi)
                : new BoxLatLonWithWraparound(latLo, latHi, lonLo, lonHi);
        } catch (IllegalArgumentException e) {
            throw new OutOfRangeException(String.format("latLo = %s, latHi = %s, lonLo = %s, lonHi = %s",
                                                        latLo, latHi, lonLo, lonHi));
        }
    }

    // Query boxes are specified as center point += delta, delta <= 360. This calculation can put us past min/max lon.
    private static double fixLon(double lon)
    {
        if (lon > Spatial.MAX_LON + CIRCLE || lon < Spatial.MIN_LON - CIRCLE) {
            throw new OutOfRangeException(String.format("longitude %s", lon));
        }
        if (lon < Spatial.MIN_LON) {
            lon += CIRCLE;
        } else if (lon > Spatial.MAX_LON) {
            lon -= CIRCLE;
        }
        return lon;
    }

    // Fix lat by truncating at +/-90
    private static double fixLat(double lat)
    {
        if (lat > Spatial.MAX_LAT + CIRCLE || lat < Spatial.MIN_LAT - CIRCLE) {
            throw new OutOfRangeException(String.format("latitude %s", lat));
        }
        if (lat > Spatial.MAX_LAT) {
            lat = Spatial.MAX_LAT;
        } else if (lat < Spatial.MIN_LAT) {
            lat = Spatial.MIN_LAT;
        }
        return lat;
    }

    private static final double CIRCLE = 360;
}
