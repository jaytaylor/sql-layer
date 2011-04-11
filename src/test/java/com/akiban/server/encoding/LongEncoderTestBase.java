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

abstract public class LongEncoderTestBase {
    static protected class TestElement {
        private final long longVal;
        private final Number number;
        private final String string;

        public TestElement(String s, Number n) {
            this.longVal = n.longValue();
            this.number = n;
            this.string = s;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s, %s)", longVal, number, string);
        }
    }

    protected final LongEncoderBase ENCODER;
    protected final TestElement[] TEST_CASES;

    public LongEncoderTestBase(LongEncoderBase encoder, TestElement[] testElements) {
        this.ENCODER = encoder;
        this.TEST_CASES = testElements;
    }

    protected String encodeAndDecode(String str) {
        final long val = ENCODER.encodeFromObject(str);
        return ENCODER.decodeToString(val);
    }


    @Test
    public void encodingToLong() {
        for(TestElement t : TEST_CASES) {
            final long encodeFromNum = ENCODER.encodeFromObject(t.number);
            final long encodeFromStr = ENCODER.encodeFromObject(t.string);
            assertEquals("Number->long: " + t, t.longVal, encodeFromNum);
            assertEquals("String->long: " + t, t.longVal, encodeFromStr);
        }
    }

    @Test
    public void decodingToString() {
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.decodeToString(t.longVal);
            assertEquals("long->String: " + t, t.string, decoded);
        }
    }

    @Test
    public void nullIsZero() {
        assertEquals(0, ENCODER.encodeFromObject(null));
    }
}
