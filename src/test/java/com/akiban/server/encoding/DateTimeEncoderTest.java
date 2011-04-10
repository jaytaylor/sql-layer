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

public class DateTimeEncoderTest {
    private class TestElement {
        private final long asLong;
        private final Object asObject;
        private final String asString;

        public TestElement(String str, Number num) {
            this.asLong = num.longValue();
            this.asObject = num;
            this.asString = str;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s, %s)", asLong, asString, asObject);
        }
    }

    private final TestElement[] TEST_CASES = {
            new TestElement("0000-00-00 00:00:00", 0),
            new TestElement("1000-01-01 00:00:00", 10000101000000L),
            new TestElement("9999-12-31 23:59:59", 99991231235959L),
            new TestElement("2011-04-10 17:04:03", Long.valueOf(20110410170403L)),
            new TestElement("1986-10-28 05:20:00", BigInteger.valueOf(19861028052000L))
    };

    private String encodeAndDecode(String dateStr) {
        final long val = DateTimeEncoder.encodeFromObject(dateStr);
        return DateTimeEncoder.decodeToString(val);
    }

    @Test
    public void encodingToInt() {
        for(TestElement t : TEST_CASES) {
            final long encodeFromNum = DateTimeEncoder.encodeFromObject(t.asObject);
            final long encodeFromStr = DateTimeEncoder.encodeFromObject(t.asString);
            assertEquals("Number->int: " + t, t.asLong, encodeFromNum);
            assertEquals("String->int: " + t, t.asLong, encodeFromStr);
        }
    }

    @Test
    public void decodingToString() {
        for(TestElement t : TEST_CASES) {
            final String decoded = DateTimeEncoder.decodeToString(t.asLong);
            assertEquals("int->String: " + t, t.asString, decoded);
        }
    }

    @Test
    public void nullIsZero() {
        assertEquals(0, DateTimeEncoder.encodeFromObject(null));
        assertEquals("0000-00-00 00:00:00", DateTimeEncoder.decodeToString(0));
    }

    @Test(expected=IllegalArgumentException.class)
    public void datePartNotNumber() {
        encodeAndDecode("2011-01-01zebra 00:00:00");
    }

    @Test(expected=IllegalArgumentException.class)
    public void timePartNotNumber() {
        encodeAndDecode("2011-01-01 00:00:00zebra");
    }

    @Test(expected=IllegalArgumentException.class)
    public void partialDatePart() {
        encodeAndDecode("2011-01 00:00:00");
    }

    @Test(expected=IllegalArgumentException.class)
    public void partialTimePart() {
        encodeAndDecode("2011-01-01 00:00");
    }

    @Test(expected=IllegalArgumentException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }

    @Test(expected=IllegalArgumentException.class)
    public void tooManyDateParts() {
        encodeAndDecode("2000-01-01-01 00:00:00");
    }

    @Test(expected=IllegalArgumentException.class)
    public void tooManyTimeParts() {
        encodeAndDecode("2000-01-01 00:00:00:00");
    }

    @Test(expected=IllegalArgumentException.class)
    public void onlyDatePart() {
        encodeAndDecode("2000-01-01");
    }

    @Test(expected=IllegalArgumentException.class)
    public void onlyTimePart() {
        encodeAndDecode("10:11:12");
    }
}
