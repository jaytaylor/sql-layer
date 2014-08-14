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

package com.foundationdb.server.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        Map<Integer,ScanFlag> ret = new TreeMap<>();
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
        Set<Integer> expected = new TreeSet<>();
        for (int i=0; i < ScanFlag.values().length; ++i) {
            expected.add(i);
        }

        Set<Integer> actual = new TreeSet<>(flagsByPosition().keySet());

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
