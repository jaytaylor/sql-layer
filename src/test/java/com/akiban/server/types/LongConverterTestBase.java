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

package com.akiban.server.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

abstract public class LongConverterTestBase {
    static protected class TestElement {
        private final long longVal;
        private final String string;

        public TestElement(String s, Number n) {
            this.longVal = n.longValue();
            this.string = s;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s)", longVal, string);
        }
    }

    protected final LongConverter ENCODER;
    protected final TestElement[] TEST_CASES;

    public LongConverterTestBase(LongConverter encoder, TestElement[] testElements) {
        this.ENCODER = encoder;
        this.TEST_CASES = testElements;
    }

    protected String encodeAndDecode(String str) {
        final long val = ENCODER.doParse(str);
        return ENCODER.asString(val);
    }

//TODO nix?
//    @Test
//    public void encodingToLong() {
//        for(TestElement t : testCases) {
//            final long encodeFromNum = converter.encodeFromObject(t.number);
//            final long encodeFromStr = converter.encodeFromObject(t.string);
//            assertEquals("Number->long: " + t, t.longVal, encodeFromNum);
//            assertEquals("String->long: " + t, t.longVal, encodeFromStr);
//        }
//    }

    @Test
    public void decodingToString() {
        int count = 0;
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.asString(t.longVal);
            assertEquals("test case [" + count + "] long->String: " + t, t.string, decoded);
            ++count;
        }
    }
//TODO nix?
//    @Test
//    public void nullIsZero() {
//        assertEquals(0, converter.encodeFromObject(null));
//    }
}
