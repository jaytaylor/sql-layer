/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
