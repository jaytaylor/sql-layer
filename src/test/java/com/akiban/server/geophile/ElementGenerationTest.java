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
