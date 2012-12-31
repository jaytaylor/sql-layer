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

import java.util.Arrays;

class Region
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('(');
        for (int d = 0; d < space.dimensions; d++) {
            if (d > 0) {
                buffer.append(", ");
            }
            buffer.append(lo(d));
            buffer.append(':');
            buffer.append(hi(d));
        }
        buffer.append(')');
        return buffer.toString();
    }

    // Region interface

    public long lo(int d)
    {
        return lo[d] + spaceLo[d];
    }

    public long hi(int d)
    {
        return hi[d] + spaceLo[d];
    }

    public boolean isPoint()
    {
        return level == space.zBits;
    }

    public void downLeft()
    {
        int d = interleave[level];
        hi[d] &= ~(1L << --xBitPosition[d]);
        level++;
    }

    public void downRight()
    {
        int d = interleave[level];
        lo[d] |= 1L << --xBitPosition[d];
        level++;
    }

    public void up()
    {
        level--;
        int d = interleave[level];
        lo[d] &= ~(1L << xBitPosition[d]);
        hi[d] |= 1L << xBitPosition[d];
        xBitPosition[d]++;
    }

    public int level()
    {
        return level;
    }

    public long z()
    {
        for (int d = 0; d < space.dimensions; d++) {
            lo[d] += spaceLo[d];
        }
        long z = space.shuffle(lo, level);
        for (int d = 0; d < space.dimensions; d++) {
            lo[d] -= spaceLo[d];
        }
        return z;
    }

    public Region copy()
    {
        return new Region(this);
    }

    public Region(Space space, long[] lo, long[] hi, int level)
    {
        this.space = space;
        this.interleave = space.interleave;
        this.lo = new long[space.dimensions];
        this.hi = new long[space.dimensions];
        this.spaceLo = space.lo;
        for (int d = 0; d < space.dimensions; d++) {
            this.lo[d] = lo[d] - spaceLo[d];
            this.hi[d] = hi[d] - spaceLo[d];
        }
        this.level = level;
        this.xBitPosition = new int[space.dimensions];
        for (int zBitPosition = space.zBits - 1; zBitPosition >= level; zBitPosition--) {
            int d = interleave[zBitPosition];
            xBitPosition[d]++;
        }
    }

    private Region(Region region)
    {
        this.space = region.space;
        this.interleave = region.interleave;
        this.lo = Arrays.copyOf(region.lo, region.lo.length);
        this.hi = Arrays.copyOf(region.hi, region.hi.length);
        this.spaceLo = region.spaceLo;
        this.level = region.level;
        this.xBitPosition = Arrays.copyOf(region.xBitPosition, region.xBitPosition.length);
    }

    private final Space space;
    private final int[] interleave;
    private final long[] lo;
    private final long[] hi;
    private final long[] spaceLo;
    private int level;
    // Region coordinates are:
    // - Zero-based: x[d] is shifted to x[d] -space.lo[d]
    // - Right-justified:  This is different from the convention in Space, because Regions are most
    //   concerned with coordinates in the user space.
    private int[] xBitPosition;
}
