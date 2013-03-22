
package com.akiban.server.geophile;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ElementGenerationTest
{
    @Test
    public void testEntireSpace()
    {
        Box2 box = new Box2(0, 1023, 0, 1023);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x0000000000000000L, 0), zs[0]);
        assertEquals(-1L, zs[1]);
        assertEquals(-1L, zs[2]);
        assertEquals(-1L, zs[3]);
    }

    @Test
    public void testLeftHalf()
    {
        Box2 box = new Box2(0, 511, 0, 1023);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x0000000000000000L, 1), zs[0]);
        assertEquals(-1L, zs[1]);
        assertEquals(-1L, zs[2]);
        assertEquals(-1L, zs[3]);
    }

    @Test
    public void testRightHalf()
    {
        Box2 box = new Box2(512, 1023, 0, 1023);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x8000000000000000L, 1), zs[0]);
        assertEquals(-1L, zs[1]);
        assertEquals(-1L, zs[2]);
        assertEquals(-1L, zs[3]);
    }

    @Test
    public void testLowerHalf()
    {
        Box2 box = new Box2(0, 1023, 0, 511);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x0000000000000000L, 2), zs[0]);
        assertEquals(space.zEncode(0x8000000000000000L, 2), zs[1]);
        assertEquals(-1L, zs[2]);
        assertEquals(-1L, zs[3]);
    }

    @Test
    public void testUpperHalf()
    {
        Box2 box = new Box2(0, 1023, 512, 1023);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x4000000000000000L, 2), zs[0]);
        assertEquals(space.zEncode(0xc000000000000000L, 2), zs[1]);
        assertEquals(-1L, zs[2]);
        assertEquals(-1L, zs[3]);
    }

    @Test
    public void testStraddlingCenter()
    {
        Box2 box = new Box2(511, 512, 511, 512);
        long[] zs = new long[4];
        space.decompose(box, zs);
        assertEquals(space.zEncode(0x3ffff00000000000L, 20), zs[0]);
        assertEquals(space.zEncode(0x6aaaa00000000000L, 20), zs[1]);
        assertEquals(space.zEncode(0x9555500000000000L, 20), zs[2]);
        assertEquals(space.zEncode(0xc000000000000000L, 20), zs[3]);
    }

    private static int[] ints(int... ints)
    {
        return ints;
    }

    private final Space space = new Space(new long[]{0, 0},
                                          new long[]{1023, 1023},
                                          ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                               0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
}
