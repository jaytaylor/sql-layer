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

package com.akiban.ais.model;

import com.akiban.server.geophile.Space;

import com.akiban.server.error.InvalidArgumentTypeException;

import java.math.BigInteger;

public class SpaceHelper
{
    /** Given the types of latitude and longitude decimal columns, return an appropriate
     * {@link Space}.
     */
    public static Space latLon(Column latCol, Column lonCol) {
        long latScale, lonScale;
        if (latCol.getType() == Types.INT)
            latScale = 1;
        else if (latCol.getType() == Types.DECIMAL)
            latScale = BigInteger.TEN.pow(latCol.getTypeParameter2().intValue()).longValue();
        else
            throw new InvalidArgumentTypeException("Latitude must be DECIMAL or INT");
        if (lonCol.getType() == Types.INT)
            lonScale = 1;
        else if (lonCol.getType() == Types.DECIMAL)
            lonScale = BigInteger.TEN.pow(lonCol.getTypeParameter2().intValue()).longValue();
        else
            throw new InvalidArgumentTypeException("Longitude must be DECIMAL or INT");

        // TODO: Maybe there should be subclasses of Space for
        // different coordinate systems?
        // Latitude is actually -90 - +90 or 0 - 180, but dims apparently have to be the same.
        // Jack says that's because I let the third argument default.
        return new Space(new long[] { -180 * latScale, -180 * lonScale },
                         new long[] { 180 * latScale, 180 * lonScale });
    }

    private SpaceHelper() {
    }
}
