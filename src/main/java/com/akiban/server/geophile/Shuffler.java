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

 Shuffling is done as follows:

 1) Scale the coordinates: For each coordinate x[d], subtract lo[d] and then shift left by shift[d].
    This gives us values that are left-justified 64-bit quantities, and therefore lined up with the shuffle masks.

 2) For each byte of each coordinate, find the mask and OR with all previously obtained masks. See the documentation
    in the constructor for an explanation of the masks.

 */

class Shuffler extends Space
{
    public long shuffle(long[] x, int length)
    {
        if (x.length != dimensions) {
            throw new IllegalArgumentException(Integer.toString(x.length));
        }
        long z = 0;
        for (int d = 0; d < dimensions; d++) {
            long xd = scaleXZ(x[d], d);
            switch (xBytes[d]) {
                case 8: z |= shuffle7[d][(int) (xd       ) & 0xff];
                case 7: z |= shuffle6[d][(int) (xd >>>  8) & 0xff];
                case 6: z |= shuffle5[d][(int) (xd >>> 16) & 0xff];
                case 5: z |= shuffle4[d][(int) (xd >>> 24) & 0xff];
                case 4: z |= shuffle3[d][(int) (xd >>> 32) & 0xff];
                case 3: z |= shuffle2[d][(int) (xd >>> 40) & 0xff];
                case 2: z |= shuffle1[d][(int) (xd >>> 48) & 0xff];
                case 1: z |= shuffle0[d][(int) (xd >>> 56) & 0xff];
            }
        }
        return zEncode(z, length);
    }

    public Shuffler(Space space)
    {
        super(space);
        long[][][] shuffles = new long[8][][];
        for (int xBytePosition = 0; xBytePosition < 8; xBytePosition++) {
            shuffles[xBytePosition] = new long[dimensions][];
            for (int d = 0; d < dimensions; d++) {
                shuffles[xBytePosition][d] = new long[256];
            }
        }
        // A z-value is computed (by shuffle(long[], int)) by combining the shuffle masks. Each mask represents
        // the contribution to the z-value by one byte of one (scaled) coordinate. The masks are computed by
        // generating all 256 byte values for each such byte, and then using xz to map the coordinate's bit
        // position to the z-value's bit position.
        for (int d = 0; d < dimensions; d++) {
            for (int xBytePosition = 0; xBytePosition < xBytes[d]; xBytePosition++) {
                for (int xByte = 0; xByte <= 0xff; xByte++) {
                    long x = ((long)xByte) << (56 - 8 * xBytePosition);
                    long mask = 1L << 63;
                    for (int xBitPosition = 0; xBitPosition < xBits[d]; xBitPosition++) {
                        if ((x & mask) != 0) {
                            int zBitPosition = 63 - xz[d][xBitPosition];
                            shuffles[xBitPosition / 8][d][xByte] |= 1L << zBitPosition;
                        }
                        mask >>>= 1;
                    }
                }
            }
        }
        shuffle0 = shuffles[0];
        shuffle1 = shuffles[1];
        shuffle2 = shuffles[2];
        shuffle3 = shuffles[3];
        shuffle4 = shuffles[4];
        shuffle5 = shuffles[5];
        shuffle6 = shuffles[6];
        shuffle7 = shuffles[7];
    }

    // A point's coordinates are x[0] .. x[dimensions - 1]. Each x[d] is 8 bytes, (bd0 .. bd7).
    // The mask of x[d]'s contributions to the shuffled value is shuffle0[d][bd0] |
    // shuffle1[d][bd1] | ... | shuffle7[d][bd7].
    private final long[][] shuffle0;
    private final long[][] shuffle1;
    private final long[][] shuffle2;
    private final long[][] shuffle3;
    private final long[][] shuffle4;
    private final long[][] shuffle5;
    private final long[][] shuffle6;
    private final long[][] shuffle7;
}
