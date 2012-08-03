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

 Unshuffling is done as follows:

 1) For each byte of each coordinate, find the mask and OR with all previously obtained masks. See the documentation
    in the constructor for an explanation of the masks.

 2) Scale the coordinates: For each coordinate x[d], shift right by shift[d] and then add lo[d].

 */

class Unshuffler extends Space
{
    public void unshuffle(long z, long[] x)
    {
        if (zLength(z) != zBits) {
            throw new IllegalArgumentException(Long.toHexString(z));
        }
        if (x.length != dimensions) {
            throw new IllegalArgumentException(Integer.toString(x.length));
        }
        z = zBits(z);
        for (int d = 0; d < dimensions; d++) {
            long xd = 0;
            switch (zBytes) {
                case 8: xd |= unshuffle7[d][(int) (z       ) & 0xff];
                case 7: xd |= unshuffle6[d][(int) (z >>>  8) & 0xff];
                case 6: xd |= unshuffle5[d][(int) (z >>> 16) & 0xff];
                case 5: xd |= unshuffle4[d][(int) (z >>> 24) & 0xff];
                case 4: xd |= unshuffle3[d][(int) (z >>> 32) & 0xff];
                case 3: xd |= unshuffle2[d][(int) (z >>> 40) & 0xff];
                case 2: xd |= unshuffle1[d][(int) (z >>> 48) & 0xff];
                case 1: xd |= unshuffle0[d][(int) (z >>> 56) & 0xff];
            }
            x[d] = scaleZX(xd, d);
        }
    }

    public Unshuffler(Space space)
    {
        super(space);
        long[][][] unshuffles = new long[8][][];
        for (int zBytePosition = 0; zBytePosition < 8; zBytePosition++) {
            unshuffles[zBytePosition] = new long[dimensions][];
            for (int d = 0; d < dimensions; d++) {
                unshuffles[zBytePosition][d] = new long[256];
            }
        }
        // A z-value is unshuffled (by shuffle(long, long[])) by combining unshuffle masks. Each mask represents
        // the contribution to a coordinate by one byte of a z-value. The masks are computed by
        // generating all 256 byte values for each such byte, and then using zx to map the coordinate's bit
        // position to the x[d]'s bit position.
        for (int zBytePosition = 0; zBytePosition < 8; zBytePosition++) {
            for (int zByte = 0; zByte <= 0xff; zByte++) {
                long z = ((long)zByte) << (56 - 8 * zBytePosition);
                long mask = 1L << 63;
                for (int zBitPosition = 0; zBitPosition < zBits; zBitPosition++) {
                    if ((z & mask) != 0) {
                        int d = interleave[zBitPosition];
                        int xBitPosition = 63 - zx[zBitPosition];
                        unshuffles[zBitPosition / 8][d][zByte] |= 1L << xBitPosition;
                    }
                    mask >>>= 1;
                }
            }
        }
        unshuffle0 = unshuffles[0];
        unshuffle1 = unshuffles[1];
        unshuffle2 = unshuffles[2];
        unshuffle3 = unshuffles[3];
        unshuffle4 = unshuffles[4];
        unshuffle5 = unshuffles[5];
        unshuffle6 = unshuffles[6];
        unshuffle7 = unshuffles[7];
    }

    // For use by this class

    private long scaleZX(long x, int d)
    {
        return (x >>> shift[d]) + lo[d];
    }

    // A z value is 8 bytes (z0 .. z7). x[d] is unshuffle0[d][z0] |
    // unshuffle0[d][z1] | ... | unshuffle7[d][z7].
    private final long[][] unshuffle0;
    private final long[][] unshuffle1;
    private final long[][] unshuffle2;
    private final long[][] unshuffle3;
    private final long[][] unshuffle4;
    private final long[][] unshuffle5;
    private final long[][] unshuffle6;
    private final long[][] unshuffle7;
}
