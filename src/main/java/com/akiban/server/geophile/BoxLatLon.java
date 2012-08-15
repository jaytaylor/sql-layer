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
        long latLo = scaleLat(latLoDecimal);
        long latHi = scaleLat(latHiDecimal.round(ROUND_UP));
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
        // Allows for query boxes
        if (lon < MIN_LON_SCALED) {
            lon += CIRCLE;
        } else if (lon > MAX_LON_SCALED) {
            lon -= CIRCLE;
        }
        return lon;
    }

    // Class state

    private static final MathContext ROUND_UP = new MathContext(0, RoundingMode.CEILING);
}
