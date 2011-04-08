/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.encoding;

import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class DateEncoderTest {
    private class TestElement {
        private final int asInt;
        private final Object asObject;
        private final String asString;

        public TestElement(String str, Number num) {
            this.asInt = num.intValue();
            this.asObject = num;
            this.asString = str;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s, %s)", asInt, asString, asObject);
        }
    }

    private final TestElement[] TEST_CASES = {
            // Zero dates
            new TestElement("0000-00-00", 0),
            new TestElement("0000-00-31", 31),
            new TestElement("0000-01-31", 63),
            new TestElement("0000-12-00", 384),
            // Valid dates
            new TestElement("1986-10-28", 1017180),
            new TestElement("2011-04-07", 1029767),
            new TestElement("1620-11-21", new Integer(829813)),
            new TestElement("0320-06-14", new Long(164046)),
            new TestElement("9999-12-25", BigInteger.valueOf(5119897))
    };

    private String encodeAndDecode(String dateStr) {
        final int val = DateEncoder.encodeFromObject(dateStr);
        return DateEncoder.decodeToString(val);
    }

    @Test
    public void encodingToInt() {
        for(TestElement t : TEST_CASES) {
            final int encodeFromNum = DateEncoder.encodeFromObject(t.asObject);
            final int encodeFromStr = DateEncoder.encodeFromObject(t.asString);
            assertEquals("Number->int: " + t, t.asInt, encodeFromNum);
            assertEquals("String->int: " + t, t.asInt, encodeFromStr);
        }
    }

    @Test
    public void decodingToString() {
        for(TestElement t : TEST_CASES) {
            final String decoded = DateEncoder.decodeToString(t.asInt);
            assertEquals("int->String: " + t, t.asString, decoded);
        }
    }

    @Test
    public void nullIsZero() {
        assertEquals(0, DateEncoder.encodeFromObject(null));
        assertEquals("0000-00-00", DateEncoder.decodeToString(0));
    }

    @Test
    public void partiallySpecified() {
        assertEquals("0002-00-00", encodeAndDecode("2"));
        assertEquals("0020-00-00", encodeAndDecode("20"));
        assertEquals("0201-00-00", encodeAndDecode("201"));
        assertEquals("2011-00-00", encodeAndDecode("2011"));
        assertEquals("2011-04-00", encodeAndDecode("2011-4"));
        assertEquals("2011-04-08", encodeAndDecode("2011-4-8"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void tooManyParts() {
        encodeAndDecode("2011-04-04-04");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidNumber() {
        encodeAndDecode("2011-04-zebra");
    }

    @Test(expected=IllegalArgumentException.class)
    public void noNumbers() {
        encodeAndDecode("a-b-c");
    }

    @Test(expected=IllegalArgumentException.class)
    public void noHyphens() {
        encodeAndDecode("banana");
    }
}
