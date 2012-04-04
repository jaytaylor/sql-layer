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

package com.akiban.server.api.dml.scan;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

public final class ScanFlagTest {

    /**
     * Test that converting int -> set -> int -> set results in the same ints and sets
     */
    @Test(timeout=10000) // in case the scan flags gets large to do this
    public void testConversionTransitivity() {
        for (int initialInt = 0, MAX = (1 << ScanFlag.values().length) -1 ; initialInt <= MAX; ++initialInt) {
            final EnumSet<ScanFlag> initialSet = ScanFlag.fromRowDataFormat(initialInt);
            final int translatedInt = ScanFlag.toRowDataFormat(initialSet);
            assertEquals("int values", initialInt, translatedInt);
            assertEquals("set values", initialSet, ScanFlag.fromRowDataFormat(translatedInt));
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void intBelowRange() {
        ScanFlag.fromRowDataFormat(-1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void intAboveRange() {
        ScanFlag.fromRowDataFormat( 1 << ScanFlag.values().length );
    }

    private static Map<Integer,ScanFlag> flagsByPosition() {
        Map<Integer,ScanFlag> ret = new TreeMap<Integer, ScanFlag>();
        for (ScanFlag flag : ScanFlag.values()) {
            ScanFlag old = ret.put(flag.getPosition(), flag);
            assertNull("conflict between " + old + " and " + flag, old);
        }
        return ret;
    }

    /**
     * Test that each enum value's position is 0 <= pos < values().length, and that none are duplicated
     */
    @Test
    public void testPositions() {
        Set<Integer> expected = new TreeSet<Integer>();
        for (int i=0; i < ScanFlag.values().length; ++i) {
            expected.add(i);
        }

        Set<Integer> actual = new TreeSet<Integer>(flagsByPosition().keySet());

        assertEquals("positions not consecutive", expected, actual);
    }

    @Test
    public void convertingEmptySet() {
        assertEquals("packed int", 0, ScanFlag.toRowDataFormat(EnumSet.noneOf(ScanFlag.class)));
    }

    @Test
    public void singleFlagConversions() {
        for (Map.Entry<Integer,ScanFlag> entry : flagsByPosition().entrySet()) {
            final int expected = 1 << entry.getKey();
            EnumSet<ScanFlag> set = EnumSet.of(entry.getValue());
            assertEquals(entry.getValue().name(), expected, ScanFlag.toRowDataFormat(set));
        }
    }
    
    /**
     * Tests one conversion, with two flags. If this works, we assume the rest will work too.
     */
    @Test
    public void testOneConversion() {
        Map<Integer,ScanFlag> flags = flagsByPosition();
        final EnumSet<ScanFlag> set = EnumSet.of(flags.get(0), flags.get(6));
        int expected = 1;
        expected |= (1 << 6);
        assertEquals("packed int for " + set, expected, ScanFlag.toRowDataFormat(set));
    }
}
