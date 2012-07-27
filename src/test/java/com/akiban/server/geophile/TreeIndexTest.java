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

import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;

public class TreeIndexTest
{
    @Before
    public void before()
    {
        space = new Space(new long[]{0, 0},
                          new long[]{X_SIZE - 1, Y_SIZE - 1},
                          ints(0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                               0, 1, 0, 1, 0, 1, 0, 1, 0, 1));
        index = new TreeIndex(space);
        long[] point = new long[2];
        for (long x = 0; x < X_SIZE; x += 10) {
            point[0] = x;
            for (long y = 0; y < Y_SIZE; y += 10) {
                point[1] = y;
                index.add(space.shuffle(point), new Point(point));
            }
        }
    }

    @Test
    public void test()
    {
        Random random = new Random(419);
        for (int i = 0; i < 10000; i++) {
            int xLo = random.nextInt(X_SIZE);
            int xHi = min(xLo + random.nextInt(X_SIZE / 4), X_SIZE - 1);
            int yLo = random.nextInt(Y_SIZE);
            int yHi = min(yLo + random.nextInt(Y_SIZE / 4), Y_SIZE - 1);
            test(xLo, xHi, yLo, yHi);
        }
    }

    private void test(int xLo, int xHi, int yLo, int yHi)
    {
        Box2 box = new Box2(xLo, xHi, yLo, yHi);
        // System.out.println(box);
        long[] zs = new long[4];
        int scanned = 0;
        int qualified = 0;
        space.decompose(box, zs);
        List<Long> actual = new ArrayList<Long>();
        for (long z : zs) {
            if (z != -1L) {
                // System.out.println(String.format("%016x", z));
                Scan scan = index.scan(z);
                SpatialObject o;
                while ((o = scan.next()) != null) {
                    Point point = (Point) o;
                    scanned++;
                    long x = point.x(0);
                    long y = point.x(1);
                    if (xLo <= x && x <= xHi && yLo <= y && y <= yHi) {
                        // System.out.println(String.format("    %s", o));
                        qualified++;
                        long p = (x << 32) | y;
                        actual.add(p);
                    }
                }
            }
        }
        List<Long> expected = new ArrayList<Long>();
        for (long x = 10 * ((xLo + 9) / 10); x <= 10 * (xHi / 10); x += 10) {
            for (long y = 10 * ((yLo + 9) / 10); y <= 10 * (yHi / 10); y += 10) {
                expected.add((x << 32) | y);
            }
        }
        Collections.sort(actual);
        Collections.sort(expected);
        assertEquals(expected, actual);
/*
        double querySize = (double) (xHi - xLo + 1) / X_SIZE *
                           (double) (yHi - yLo + 1) / Y_SIZE;
        double accuracy = (double) qualified / scanned;
        System.out.println(String.format("query size: %6.4f\taccuracy: %6.4f", querySize, accuracy));
*/
    }

    private int[] ints(int ... ints)
    {
        return ints;
    }

    private static final int X_SIZE = 0x400;
    private static final int Y_SIZE = 0x400;

    private Space space;
    private Index index;
}
