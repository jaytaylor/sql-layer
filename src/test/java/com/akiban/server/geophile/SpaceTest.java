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

import com.akiban.server.geophile.Space;
import org.junit.Test;
import static org.junit.Assert.*;

public class SpaceTest
{
    @Test
    public void testSquare2DSpace()
    {
        Space space = new Space(new long[]{0x000, 0x000},
                                new long[]{0x3ff, 0x3ff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        check(space, 0x0000000000000000L, longs(0x000, 0x000));
        check(space, 0x5555500000000000L, longs(0x000, 0x3ff));
        check(space, 0xaaaaa00000000000L, longs(0x3ff, 0x000));
        check(space, 0xfffff00000000000L, longs(0x3ff, 0x3ff));
        check(space, 0x4000100000000000L, longs(0x000, 0x201));
        check(space, 0x8000200000000000L, longs(0x201, 0x000));
        check(space, 0xc000300000000000L, longs(0x201, 0x201));
    }

    @Test
    public void testRectangular2DSpace()
    {
        Space space = new Space(new long[]{0x000, 0x000},
                                new long[]{0x3ff, 0xfff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     1, 1));
        check(space, 0x0000000000000000L, longs(0x000, 0x000));
        check(space, 0x55555c0000000000L, longs(0x000, 0xfff));
        check(space, 0xaaaaa00000000000L, longs(0x3ff, 0x000));
        check(space, 0xfffffc0000000000L, longs(0x3ff, 0xfff));
        check(space, 0x4000040000000000L, longs(0x000, 0x801));
        check(space, 0x8000200000000000L, longs(0x201, 0x000));
        check(space, 0xc000240000000000L, longs(0x201, 0x801));
    }

    @Test
    public void test3DSpace()
    {
        Space space = new Space(new long[]{0x000, 0x000, 0x000},
                                new long[]{0x3ff, 0x3ff, 0x3ff},
                                ints(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2,
                                     0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2));
        check(space, 0x0000000000000000L, longs(0x000, 0x000, 0x000));
        check(space, 0x2492492400000000L, longs(0x000, 0x000, 0x3ff));
        check(space, 0x4924924800000000L, longs(0x000, 0x3ff, 0x000));
        check(space, 0x6db6db6c00000000L, longs(0x000, 0x3ff, 0x3ff));
        check(space, 0x9249249000000000L, longs(0x3ff, 0x000, 0x000));
        check(space, 0xb6db6db400000000L, longs(0x3ff, 0x000, 0x3ff));
        check(space, 0xdb6db6d800000000L, longs(0x3ff, 0x3ff, 0x000));
        check(space, 0xfffffffc00000000L, longs(0x3ff, 0x3ff, 0x3ff));
        check(space, 0x2000000400000000L, longs(0x000, 0x000, 0x201));
        check(space, 0x4000000800000000L, longs(0x000, 0x201, 0x000));
        check(space, 0x6000000c00000000L, longs(0x000, 0x201, 0x201));
        check(space, 0x8000001000000000L, longs(0x201, 0x000, 0x000));
        check(space, 0xa000001400000000L, longs(0x201, 0x000, 0x201));
        check(space, 0xc000001800000000L, longs(0x201, 0x201, 0x000));
        check(space, 0xe000001c00000000L, longs(0x201, 0x201, 0x201));
    }

    @Test
    public void testNonZeroLo()
    {
        final int LO_0 = -123;
        final int LO_1 = 456;
        Space space = new Space(new long[]{LO_0, LO_1},
                                new long[]{LO_0 + 0x3ff, LO_1 + 0x3ff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        check(space, 0x0000000000000000L, longs(LO_0 + 0x000, LO_1 + 0x000));
        check(space, 0x5555500000000000L, longs(LO_0 + 0x000, LO_1 + 0x3ff));
        check(space, 0xaaaaa00000000000L, longs(LO_0 + 0x3ff, LO_1 + 0x000));
        check(space, 0xfffff00000000000L, longs(LO_0 + 0x3ff, LO_1 + 0x3ff));
        check(space, 0x4000100000000000L, longs(LO_0 + 0x000, LO_1 + 0x201));
        check(space, 0x8000200000000000L, longs(LO_0 + 0x201, LO_1 + 0x000));
        check(space, 0xc000300000000000L, longs(LO_0 + 0x201, LO_1 + 0x201));
    }

    @Test
    public void siblings()
    {
        Space space = new Space(new long[]{0x000, 0x000},
                                new long[]{0x3ff, 0x3ff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        long zRoot = space.zEncode(0x0000000000000000L, 0);
        long z0 = space.zEncode(0x0000000000000000L, 1);
        long z1 = space.zEncode(0x8000000000000000L, 1);
        assertFalse(space.siblings(zRoot, z0));
        assertFalse(space.siblings(z0, z0));
        assertFalse(space.siblings(z1, z1));
        assertTrue(space.siblings(z0, z1));
        assertTrue(space.siblings(z1, z0));
        long z00 = space.zEncode(0x0000000000000000L, 2);
        long z01 = space.zEncode(0x4000000000000000L, 2);
        long z10 = space.zEncode(0x8000000000000000L, 2);
        long z11 = space.zEncode(0xc000000000000000L, 2);
        assertFalse(space.siblings(z00, z0));
        assertFalse(space.siblings(z00, z00));
        assertTrue(space.siblings(z00, z01));
        assertFalse(space.siblings(z00, z10));
        assertFalse(space.siblings(z00, z11));
        assertTrue(space.siblings(z01, z00));
        assertFalse(space.siblings(z01, z01));
        assertFalse(space.siblings(z01, z10));
        assertFalse(space.siblings(z01, z11));
        assertFalse(space.siblings(z10, z00));
        assertFalse(space.siblings(z10, z01));
        assertFalse(space.siblings(z10, z10));
        assertTrue(space.siblings(z10, z11));
        assertFalse(space.siblings(z11, z00));
        assertFalse(space.siblings(z11, z01));
        assertTrue(space.siblings(z11, z10));
        assertFalse(space.siblings(z11, z11));
        long zLeaf = space.zEncode(0x1234500000000000L, 20);
        long zLeafSibling = space.zEncode(0x1234400000000000L, 20);
        assertTrue(space.siblings(zLeaf, zLeafSibling));
    }

    @Test
    public void parent()
    {
        Space space = new Space(new long[]{0x000, 0x000},
                                new long[]{0x3ff, 0x3ff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        assertTrue(space.zEncode(0x1234500000000000L, 20) != space.parent(space.zEncode(0x1234500000000000L, 20)));
        assertEquals(space.zEncode(0x1234400000000000L, 19), space.parent(space.zEncode(0x1234500000000000L, 20)));
        assertEquals(space.zEncode(0x1234400000000000L, 18), space.parent(space.zEncode(0x1234400000000000L, 19)));
        assertEquals(space.zEncode(0x1234000000000000L, 17), space.parent(space.zEncode(0x1234400000000000L, 18)));
        assertEquals(space.zEncode(0x1234000000000000L, 16), space.parent(space.zEncode(0x1234000000000000L, 17)));
        assertEquals(space.zEncode(0x1234000000000000L, 15), space.parent(space.zEncode(0x1234000000000000L, 16)));
        assertEquals(space.zEncode(0x1234000000000000L, 14), space.parent(space.zEncode(0x1234000000000000L, 15)));
        assertEquals(space.zEncode(0x1230000000000000L, 13), space.parent(space.zEncode(0x1234000000000000L, 14)));
        assertEquals(space.zEncode(0x1230000000000000L, 12), space.parent(space.zEncode(0x1230000000000000L, 13)));
        assertEquals(space.zEncode(0x1220000000000000L, 11), space.parent(space.zEncode(0x1230000000000000L, 12)));
        assertEquals(space.zEncode(0x1200000000000000L, 10), space.parent(space.zEncode(0x1220000000000000L, 11)));
        assertEquals(space.zEncode(0x1200000000000000L,  9), space.parent(space.zEncode(0x1200000000000000L, 10)));
        assertEquals(space.zEncode(0x1200000000000000L,  8), space.parent(space.zEncode(0x1200000000000000L,  9)));
        assertEquals(space.zEncode(0x1200000000000000L,  7), space.parent(space.zEncode(0x1200000000000000L,  8)));
        assertEquals(space.zEncode(0x1000000000000000L,  6), space.parent(space.zEncode(0x1200000000000000L,  7)));
        assertEquals(space.zEncode(0x1000000000000000L,  5), space.parent(space.zEncode(0x1000000000000000L,  6)));
        assertEquals(space.zEncode(0x1000000000000000L,  4), space.parent(space.zEncode(0x1000000000000000L,  5)));
        assertEquals(space.zEncode(0x0000000000000000L,  3), space.parent(space.zEncode(0x1000000000000000L,  4)));
        assertEquals(space.zEncode(0x0000000000000000L,  2), space.parent(space.zEncode(0x0000000000000000L,  3)));
        assertEquals(space.zEncode(0x0000000000000000L,  1), space.parent(space.zEncode(0x0000000000000000L,  2)));
        assertEquals(space.zEncode(0x0000000000000000L,  0), space.parent(space.zEncode(0x0000000000000000L,  1)));
    }

    @Test
    public void contains()
    {
        Space space = new Space(new long[]{0x000, 0x000},
                                new long[]{0x3ff, 0x3ff},
                                ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                     0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        long z = space.zEncode(0xfedcb00000000000L, 20);
        assertTrue (space.contains(space.zEncode(0x0000000000000000L, 0), z));
        assertTrue (space.contains(space.zEncode(0x8000000000000000L, 1), z));
        assertFalse(space.contains(space.zEncode(0x0000000000000000L, 1), z));
        assertTrue (space.contains(space.zEncode(0xc000000000000000L, 2), z));
        assertFalse(space.contains(space.zEncode(0x8000000000000000L, 2), z));
        assertTrue (space.contains(space.zEncode(0xe000000000000000L, 3), z));
        assertFalse(space.contains(space.zEncode(0xc000000000000000L, 3), z));
        assertTrue (space.contains(space.zEncode(0xf000000000000000L, 4), z));
        assertFalse(space.contains(space.zEncode(0xe000000000000000L, 4), z));
        assertTrue (space.contains(space.zEncode(0xf800000000000000L, 5), z));
        assertFalse(space.contains(space.zEncode(0xf000000000000000L, 5), z));
        assertTrue (space.contains(space.zEncode(0xfc00000000000000L, 6), z));
        assertFalse(space.contains(space.zEncode(0xf800000000000000L, 6), z));
        assertTrue (space.contains(space.zEncode(0xfe00000000000000L, 7), z));
        assertFalse(space.contains(space.zEncode(0xfc00000000000000L, 7), z));
        assertTrue (space.contains(space.zEncode(0xfe00000000000000L, 8), z));
        assertFalse(space.contains(space.zEncode(0xff00000000000000L, 8), z));
        assertTrue (space.contains(space.zEncode(0xfe80000000000000L, 9), z));
        assertFalse(space.contains(space.zEncode(0xfe00000000000000L, 9), z));
        assertTrue (space.contains(space.zEncode(0xfec0000000000000L, 10), z));
        assertFalse(space.contains(space.zEncode(0xfe80000000000000L, 10), z));
        assertTrue (space.contains(space.zEncode(0xfec0000000000000L, 11), z));
        assertFalse(space.contains(space.zEncode(0xfee0000000000000L, 11), z));
        assertTrue (space.contains(space.zEncode(0xfed0000000000000L, 12), z));
        assertFalse(space.contains(space.zEncode(0xfec0000000000000L, 12), z));
        assertTrue (space.contains(space.zEncode(0xfed8000000000000L, 13), z));
        assertFalse(space.contains(space.zEncode(0xfed0000000000000L, 13), z));
        assertTrue (space.contains(space.zEncode(0xfedc000000000000L, 14), z));
        assertFalse(space.contains(space.zEncode(0xfed8000000000000L, 14), z));
        assertTrue (space.contains(space.zEncode(0xfedc000000000000L, 15), z));
        assertFalse(space.contains(space.zEncode(0xfede000000000000L, 15), z));
        assertTrue (space.contains(space.zEncode(0xfedc000000000000L, 16), z));
        assertFalse(space.contains(space.zEncode(0xfedd000000000000L, 16), z));
        assertTrue (space.contains(space.zEncode(0xfedc800000000000L, 17), z));
        assertFalse(space.contains(space.zEncode(0xfedc000000000000L, 17), z));
        assertTrue (space.contains(space.zEncode(0xfedc800000000000L, 18), z));
        assertFalse(space.contains(space.zEncode(0xfedcc00000000000L, 18), z));
        assertTrue (space.contains(space.zEncode(0xfedca00000000000L, 19), z));
        assertFalse(space.contains(space.zEncode(0xfedc800000000000L, 19), z));
        assertTrue (space.contains(space.zEncode(0xfedcb00000000000L, 20), z));
        assertFalse(space.contains(space.zEncode(0xfedca00000000000L, 20), z));
    }

    private static long[] longs(long ... longs)
    {
        return longs;
    }

    private static int[] ints(int ... ints)
    {
        return ints;
    }

    private void check(Space space, long shuffled, long[] x)
    {
        assertEquals(space.zEncode(shuffled, space.zBits), space.shuffle(x));
        long[] unshuffled = new long[x.length];
        space.unshuffle(space.zEncode(shuffled, space.zBits), unshuffled);
        assertArrayEquals(x, unshuffled);
    }
}
