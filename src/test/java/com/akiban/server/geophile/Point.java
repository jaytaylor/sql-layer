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

import com.akiban.server.geophile.Region;
import com.akiban.server.geophile.RegionComparison;
import com.akiban.server.geophile.SpatialObject;

import java.util.Arrays;

public class Point implements SpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        boolean first = true;
        for (long a : x) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(a);
        }
        buffer.append(')');
        return buffer.toString();
    }

    // SpatialObject interface

    public long[] arbitraryPoint()
    {
        return x;
    }

    public boolean containedBy(Region region)
    {
        int dimensions = x.length;
        long[] rLo = region.lo();
        long[] rHi = region.hi();
        assert rLo.length == dimensions;
        for (int d = 0; d < dimensions; d++) {
            long xd = x[d];
            if (xd < rLo[d] || xd > rHi[d]) {
                return false;
            }
        }
        return true;
    }

    public RegionComparison compare(Region region)
    {
        // Points should only be shuffled on insert, not indexed.
        throw new UnsupportedOperationException();
    }

    // Point interface

    public long x(int d)
    {
        return x[d];
    }

    public Point(long[] x)
    {
        this.x = Arrays.copyOf(x, x.length);
    }

    // Object state

    private final long[] x;
}
