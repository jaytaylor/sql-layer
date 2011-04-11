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

import static org.junit.Assert.assertEquals;

public class YearEncoderTest {
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

    private final LongEncoderBase ENCODER = EncoderFactory.YEAR;

    private final TestElement[] TEST_CASES = {
            new TestElement("0000", 0),
            new TestElement("1901", 1),
            new TestElement("1950", 50),
            new TestElement("2000", 100),
            new TestElement("2028", 128),
            new TestElement("2029", 129),
            new TestElement("2155", 255),
            new TestElement("2011", new Integer(111)),
            new TestElement("1986", new Long(86))
    };

    private String encodeAndDecode(String dateStr) {
        final long val = ENCODER.encodeFromObject(dateStr);
        return ENCODER.decodeToString(val);
    }

    @Test
    public void encodingToInt() {
        for(TestElement t : TEST_CASES) {
            final long encodeFromNum = ENCODER.encodeFromObject(t.asObject);
            final long encodeFromStr = ENCODER.encodeFromObject(t.asString);
            assertEquals("Number->int: " + t, t.asInt, encodeFromNum);
            assertEquals("String->int: " + t, t.asInt, encodeFromStr);
        }
    }

    @Test
    public void decodingToString() {
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.decodeToString(t.asInt);
            assertEquals("int->String: " + t, t.asString, decoded);
        }
    }

    @Test
    public void nullIsZero() {
        assertEquals(0, ENCODER.encodeFromObject(null));
        assertEquals("0000", ENCODER.decodeToString(0));
    }

    @Test
    public void partiallySpecified() {
        assertEquals("0002", encodeAndDecode("2"));
        assertEquals("0020", encodeAndDecode("20"));
        assertEquals("0201", encodeAndDecode("201"));
        assertEquals("2011", encodeAndDecode("2011"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidNumber() {
        encodeAndDecode("20111zebra");
    }

    @Test(expected=IllegalArgumentException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }
}
