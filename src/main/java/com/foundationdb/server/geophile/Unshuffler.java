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
