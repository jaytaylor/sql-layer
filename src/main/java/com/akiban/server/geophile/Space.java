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

/**

 The Space class and its subclasses implements the core logic of z-order based spatial indexes. The Space class
 contains metadata on the space and on the mapping to z-order. The following subclasses implement major functions
 on spaces and contain additional metadata:
 - Shuffler: Shuffling refers to the process of interleaving a point's coordinates to obtain a z-value of maximum
   resolution.
 - Unshuffler: Translates a z-value back to a point.
 - Decomposer: Generalization of Shuffler, mapping a spatial object into a set of z-values.

 This documentation describes the metadata in this class, and its derivation. While it is self-contained, you're
 probably better off starting with the wikipedia article on z-order: http://en.wikipedia.org/wiki/Z-order_(curve), or
 even better: Orenstein & Manola, PROBE Spatial Data Modeling and Query Processing in an Image Database Application.
 IEEE Trans. Software Eng. 14(5): 611-629 (1988).

 A Space is created using the Space(long[] lo, long[] hi, long[] interleave) constructor.
 lo/hi give the bounds of the space, so the size of these array is the number of dimensions in the space.
 Both the lo and hi bounds are inclusive. So if you want to create a 2-d space
 whose lower-left coordinate is (0, 0) and whose upper-right coordinate is (999, 999), then:
 - lo = {0L, 0L}
 - hi = {999L, 999L}

 interleave describes how the bits of a point are interleaved to form a z-value. Continuing with the previous example:
 A point in the (0:999, 0:999) space can be described by a pair of numbers, (x, y), each of which require 10 bits.
 I.e., the values in the range 0-999 can be represented by a 10-bit number. For example, the point (0, 255) would be
 represented by (0b0000000000, 0b0011111111), (to use the Java 7 binary literal notation). A z-value is formed by
 interleaving these bits. If we want to interleave in as unbiased a way as possible, the pattern would be
 (x, y, x, y, x, y, x, y, x, y, x, y, x, y, x, y, x, y, x, y). Referring to x as dimension 0, and y as dimension 1
 gives us interleave = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}. Actually, because we start with
 an x bit, the interleave favors x slightly. The most unbiased interleaving favoring y would be
 {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0}.

 Given an (x, y) point, we could actually compute a z-value by going through the interleave array, masking out the
 next bit from x or y, and setting the corresponding bit in a z-value. But shuffling and unshuffling need to be as fast
 as possible since they are such common operations, (shuffling at least). This implementation computes masks that can
 operates one byte at a time.

 The fields of this class are used in shuffling, unshuffling and decomposing spatial objects. Notation conventions:
 - d refers to a dimension, 0 .. dimensions - 1
 - x refers to a coordinate. The coordinates of a point are x[0] .. x[dimensions-1]
 - z refers to a z value
 - Bit positions are numbered from 0, starting at the left (MSB).

 Summary of fields:

 - xBits[d] is the number of bits needed to represent a coordinate of dimension d. In the example above,
   xBits[0] = xBits[1] = 10.

 - xBytes[d] is xBits[d] rounded up to the nearest byte. Shuffling needs this number frequently, so it is stored
   instead of being derived from xBits[d] when needed.

  - A z-value occupies zBits bits.

  - zBytes is zBits rounded up to the nearest byte.

 - Z-values are stored left-justified in longs. This is because z-values compare lexicographically. i.e., we want
   0b0011 to rank before 0b00110. But coordinates are right-justified. shift[d] is the amount by which x[d] - lo[d]
   needs to be shifted before shuffling.

 - Shuffling is accomplished by taking bytes from coordinates and indexing into an array of masks. OR-ing the masks
   together creates the z-value. xz is an intermediate needed to compute the masks. xz[d][p] is the bit position
   within the z-value of the pth bit position of x[d], 0 <= p < xBits[d].

 - Similarly, unshuffling needs a mapping from z bit positions to x bit positions, and zx provides it. zx[p] is
   the position within x[interleave[p]] of z bit position p, 0 <= p < zBits.

  See the Shuffler, Unshuffler, and Decomposer classes for information on how xz, zx and shift are used to map between
  coordinates and z-values.

 */

import java.util.Arrays;

public class Space
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        for (int d = 0; d < dimensions; d++) {
            if (d > 0) {
                buffer.append(", ");
            }
            buffer.append(lo[d]);
            buffer.append(':');
            buffer.append(hi[d]);
        }
        buffer.append(']');
        return buffer.toString();
    }


    // Space interface

    /**
     * Returns the dimensionality of the space.
     * @return the dimensionality of the space.
     */
    public int dimensions()
    {
        return dimensions;
    }

    /**
     * Compute the z-value for the given coordinates. The length of the z-value is that of the maximum resolution
     * for this space.
     * @param x Coordinates of point to be shuffled.
     * @return A z-value.
     */
    public long shuffle(long[] x)
    {
        return shuffler.shuffle(x, zBits);
    }

    /**
     * Compute the z-value for the given coordinates. length indicates the number of bits in the z-value.
     * @param x Coordinates of point to be shuffled.
     * @param length Number of bits in the z-value.
     * @return A z-value.
     */
    public long shuffle(long[] x, int length)
    {
        return shuffler.shuffle(x, length);
    }

    /**
     * Unshuffle the given z-value, setting the coordinates in x.
     * @param z A z-value
     * @param x The coordinates corresponding to the z-value.
     */
    public void unshuffle(long z, long[] x)
    {
        unshuffler.unshuffle(z, x);
    }

    /**
     * Decompose a spatial object, depositing the z-values in zs. The maximum number of z-values is z.length. (So
     * if more precision is needed, pass in a larger array.)
     * @param spatialObject The spatial object to be decomposed.
     * @param zs Z-values will be written into this array. If zs.length z-values are not required, unused slots will
     *           contain -1L. All of the unused slots will occur at the end of the array.
     */
    public void decompose(SpatialObject spatialObject, long[] zs)
    {
        decomposer.decompose(spatialObject, zs);

    }

    /**
     * The lowest z-value (interpreted as an integer) in the region covered by the given z-value.
     * @param z A z-value
     * @return The lowest z-value (interpreted as an integer) in the region covered by the given z-value.
     */
    public long zLo(long z)
    {
        return z;
    }

    /**
     * The lowest z-value (interpreted as an integer) in the region covered by the given z-value.
     * @param z A z-value
     * @return The lowest z-value (interpreted as an integer) in the region covered by the given z-value.
     */
    public long zHi(long z)
    {
        // Fill the unused bits of the z-value with 1s (but leave the length intact).
        int length = zLength(z);
        long mask = ((1L << (MAX_LENGTH - length)) - 1) << LENGTH_BITS;
        return z | mask;
    }

    public Space(long[] lo, long[] hi)
    {
        this(lo, hi, null);
    }

    public Space(long[] lo, long[] hi, int[] interleave)
    {
        this.dimensions = lo.length;
        this.lo = Arrays.copyOf(lo, lo.length);
        this.hi = Arrays.copyOf(hi, hi.length);
        this.xBits = new int[dimensions];
        this.xBytes = new int[dimensions];
        this.shift = new int[dimensions];
        this.xz = new int[dimensions][];
        this.zBits = computeDimensionBoundaries();
        this.interleave =
            interleave == null
            ? defaultInterleave()
            : Arrays.copyOf(interleave, interleave.length);
        this.zBytes = (zBits + 7) / 8;
        this.zx = new int[zBits];
        initializeZXMapping();
        this.shuffler = new Shuffler(this);
        this.unshuffler = new Unshuffler(this);
        this.decomposer = new Decomposer(this);
        checkArguments(lo, hi);
    }

    // For use by this package

    boolean siblings(long a, long b)
    {
        boolean neighbors = false;
        int length = zLength(a);
        if (length > 0 && length == zLength(b)) {
            // siblings agree in the first length - 1 bits and have
            // opposite values in the next bit.
            long matchMask = ((1L << (length - 1)) - 1) << (64 - length);
            long differMask = 1L << (63 - length);
            neighbors =
                (a & matchMask) == (b & matchMask) &&
                ((a & differMask) ^ (b & differMask)) != 0;
        }
        return neighbors;
    }

    long parent(long z)
    {
        int length = zLength(z);
        if (length == 0) {
            throw new IllegalArgumentException(Long.toString(z));
        }
        length--;
        long mask = ((1L << length) - 1) << (63 - length);
        return (z & mask) | length;
    }

    long zEncode(long z, long length)
    {
        assert (z & LENGTH_MASK) == 0 : z;
        assert length <= MAX_LENGTH : length;
        return (z >>> 1) | length;
    }

    long zBits(long z)
    {
        return (z & ~LENGTH_MASK) << 1;
    }

    int zLength(long z)
    {
        return (int) (z & LENGTH_MASK);
    }

    // Returns true iff a contains b
    boolean contains(long a, long b)
    {
        int aLength = zLength(a);
        int bLength = zLength(b);
        boolean contains = aLength <= bLength;
        if (contains) {
            long mask = ((1L << aLength) - 1) << (63 - aLength);
            contains = (a & mask) == (b & mask);
        }
        return contains;
    }

    // For use by subclasses

    protected Space(Space space)
    {
        this.dimensions = space.dimensions;
        this.lo = space.lo;
        this.hi = space.hi;
        this.interleave = space.interleave;
        this.xz = space.xz;
        this.zx = space.zx;
        this.shift = space.shift;
        this.xBits = space.xBits;
        this.xBytes = space.xBytes;
        this.zBits = space.zBits;
        this.zBytes = space.zBytes;
        this.shuffler = space.shuffler;
        this.unshuffler = space.unshuffler;
        this.decomposer = space.decomposer;
    }

    // For use by this class

    private void checkArguments(long[] lo, long[] hi)
    {
        int dimensions = lo.length;
        if (hi.length != dimensions) {
            throw new IllegalArgumentException(String.format("lo.length = %s, hi.length = %s",
                                                             dimensions, hi.length));
        }
        if (dimensions < 1 || dimensions > MAX_DIMENSIONS) {
            throw new IllegalArgumentException(
                String.format("dimensions must be between 1 and %s inclusive", MAX_DIMENSIONS));
        }
        for (int d = 0; d < dimensions; d++) {
            if (lo[d] >= hi[d]) {
                throw new IllegalArgumentException(
                    String.format("Invalid dimension %s: lo = %s, hi = %s", d, lo[d], hi[d]));
            }
        }
        if (interleave.length != zBits) {
            throw new IllegalArgumentException(String.format("Length of interleave array must be zBits (%s)", zBits));
        }
        for (int zBitPosition = 0; zBitPosition < zBits; zBitPosition++) {
            int d = interleave[zBitPosition];
            if (d < 0 || d >= dimensions) {
                throw new IllegalArgumentException(
                    String.format("interleave[%s] is %s. Must refer to a dimension, 0 .. %s",
                                  zBitPosition, d, dimensions - 1));
            }
        }
    }

    private int computeDimensionBoundaries()
    {
        int zBits = 0;
        for (int d = 0; d < dimensions; d++) {
            long n = hi[d] - lo[d] + 1;
            while ((1L << xBits[d]) < n) {
                xBits[d]++;
            }
            shift[d] = 64 - xBits[d];
            xBytes[d] = (xBits[d] + 7) / 8;
            zBits += xBits[d];
            xz[d] = new int[xBits[d]];
        }
        return zBits;
    }

    private void initializeZXMapping()
    {
        int[] xPosition = new int[dimensions];
        for (int zBitPosition = 0; zBitPosition < zBits; zBitPosition++) {
            int d = interleave[zBitPosition];
            int xBitPosition = xPosition[d];
            xz[d][xBitPosition] = zBitPosition;
            zx[zBitPosition] = xBitPosition;
            xPosition[d]++;
        }
    }

    private int[] defaultInterleave()
    {
        int[] interleave = new int[zBits];
        for (int zBitPosition = 0; zBitPosition < zBits; zBitPosition++) {
            interleave[zBitPosition] = zBitPosition % dimensions;
        }
        return interleave;
    }

    // Class state

    private static final int MAX_DIMENSIONS = 6;
    // Format of a z:
    // MSB  0: 0
    //   1-57: left-justified bits
    //  58-63: length
    private static final int MAX_LENGTH = 57;
    private static final long LENGTH_MASK = 0x3f;
    private static final int LENGTH_BITS = 6;

    // Object state

    // Dimensions of the space
    protected final int dimensions;
    // Bounds of the space
    protected final long[] lo;
    protected final long[] hi;
    // Interleave pattern
    protected final int[] interleave;
    // xz translates x bit positions to z bit positions. xz[d][xp] is the z bit position of x[d]
    // bit position xp.
    protected final int[][] xz;
    // zx translates z bit positions to x bit positions. zx[zp] is the position of zp in
    // x[interleave[zp]].
    protected final int[] zx;
    // Number of bits to left-shift coordinates in preparation for shuffling.
    protected final int[] shift;
    // xBits[d] is the number of bits needed to represent a coordinate of dimension d.
    protected final int[] xBits;
    // xBytes[d] is xBits[d] the number of bytes needed to represent a coordinate of dimension d;
    protected final int[] xBytes;
    // Number of bits in a z-value        1
    protected final int zBits;
    // Number of bytes in a z-value
    protected final int zBytes;
    private final Shuffler shuffler;
    private final Unshuffler unshuffler;
    private final Decomposer decomposer;
}
