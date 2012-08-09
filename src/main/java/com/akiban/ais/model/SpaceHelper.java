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
        if ((latCol.getType() != Types.DECIMAL) ||
            (lonCol.getType() != Types.DECIMAL)) {
            throw new InvalidArgumentTypeException("Columns must both be DECIMAL");
        }
        long latScale = BigInteger.TEN.pow(latCol.getTypeParameter2().intValue()).longValue();
        long lonScale = BigInteger.TEN.pow(lonCol.getTypeParameter2().intValue()).longValue();
        // TODO: Maybe there should be subclasses of Space for
        // different coordinate systems?
        // Latitude is actually -90 - +90 or 0 - 180, but dims apparently have to be the same.
        return new Space(new long[] { 0, 0 },
                         new long[] { 360 * latScale, 360 * lonScale });
    }

    private SpaceHelper() {
    }
}
