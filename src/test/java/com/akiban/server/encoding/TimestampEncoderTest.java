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

public class TimestampEncoderTest {
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

    private LongEncoderBase ENCODER = EncoderFactory.TIMESTAMP;

    private final TestElement[] TEST_CASES = {
            new TestElement("0", 0),                                // 1970-01-01 00:00:00
            new TestElement("1234567890", 1234567890),              // 2009-02-13 23:31:30
            new TestElement("2147483647", 2147483647),              // 2038-01-19 03:14:07
            new TestElement("530841600", new Integer(530841600)),   // 1986-10-28 00:00:00
            new TestElement("1302460440", new Long(1302460440))     // 2011-04-10 18:34:00
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
        assertEquals("0", ENCODER.decodeToString(0));
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
