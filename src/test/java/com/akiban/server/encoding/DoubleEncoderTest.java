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
//
//package com.akiban.server.encoding;
//
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//
//public final class DoubleEncoderTest {
//    static private class TestElement {
//        private final double dbl;
//        private final String str;
//        private final long longBits;
//
//        public TestElement(double dbl, long bits) {
//            this.dbl = dbl;
//            this.str = Double.valueOf(dbl).toString();
//            this.longBits = bits;
//        }
//
//        @Override
//        public String toString() {
//            return String.format("(%s, %f, %d)", str, dbl, longBits);
//        }
//    }
//
//    static private final double EPSILON = 0;
//
//    private final TestElement[] TEST_CASES = {
//            new TestElement(                       -0.0d, 0x8000000000000000L),
//            new TestElement(                        0.0d, 0x0000000000000000L),
//            new TestElement(                       -1.0d, 0xBFF0000000000000L),
//            new TestElement(                        1.0d, 0x3FF0000000000000L),
//            new TestElement(   839573957392.29575739275d, 0x42686F503D620977L),
//            new TestElement(            -0.986730586093d, 0xBFEF934C05A76F64L),
//            new TestElement(428732459843.84344482421875d, 0x4258F49C8AD0F5FBL),
//            new TestElement(               2.7182818284d, 0x4005BF0A8B12500BL),
//            new TestElement(          -9007199250000000d, 0xC33FFFFFFFB7A880L),
//            new TestElement(        7385632847582937583d, 0x43D99FC27C6C68D0L)
//    };
//
//    @Test
//    public void encodeToBits() {
//        for(TestElement t : TEST_CASES) {
//            final long bitsFromFloat = DoubleEncoder.encodeFromObject(t.dbl);
//            final long bitsFromString = DoubleEncoder.encodeFromObject(t.str);
//            assertEquals("float->bits: " + t, t.longBits, bitsFromFloat);
//            assertEquals("string->bits: " + t, t.longBits, bitsFromString);
//        }
//    }
//
//    @Test
//    public void decodeToDouble() {
//        for(TestElement t : TEST_CASES) {
//            final double decodeFromBits = DoubleEncoder.decodeFromBits(t.longBits);
//            assertEquals("bits->float: " + t, t.dbl, decodeFromBits, EPSILON);
//        }
//    }
//
//    @Test
//    public void nullIsZero() {
//        assertEquals("null not encoded to 0", 0, DoubleEncoder.encodeFromObject(null), EPSILON);
//    }
//
//    @Test(expected=IllegalArgumentException.class)
//    public void invalidNumber() {
//        DoubleEncoder.encodeFromObject("zebra");
//    }
//}
