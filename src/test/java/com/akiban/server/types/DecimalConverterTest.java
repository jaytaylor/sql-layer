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
//package com.akiban.server.types;
//
//import static junit.framework.Assert.assertEquals;
//
//import java.util.Arrays;
//import java.util.List;
//
//import org.junit.Test;
//
//public final class DecimalConverterTest {
//    @Test
//    public void bug687048() {
//        class TestElement {
//            private final int precision;
//            private final int scale;
//            private final String asNumber;
//            private final String asHex;
//
//            public TestElement(int precision, int scale, String asNumber, String asHex) {
//                this.precision = precision;
//                this.scale = scale;
//                this.asNumber = asNumber;
//                this.asHex = asHex;
//            }
//
//            @Override
//            public String toString() {
//                return String.format("(%d, %d, %s, %s)", precision, scale, asNumber, asHex);
//            }
//        }
//
//        List<TestElement> tests = Arrays.asList(
//            // Values that were in the c_discount decimal(4,2) field
//            new TestElement(4, 2, "0.38", "0x8026"),
//            new TestElement(4, 2, "0.44", "0x802C"),
//            new TestElement(4, 2, "0.01", "0x8001"),
//            new TestElement(4, 2, "0.33", "0x8021"),
//            new TestElement(4, 2, "0.04", "0x8004"),
//            new TestElement(4, 2, "0.50", "0x8032"),
//            new TestElement(4, 2, "0.45", "0x802D"),
//            new TestElement(4, 2, "0.14", "0x800E"),
//            new TestElement(4, 2, "0.03", "0x8003"),
//
//            // Values that were in the c_balance decimal(12,2) field
//            new TestElement(12, 2, "4673.96", "0x800000124160"),
//            new TestElement(12, 2, "8028.00", "0x8000001F5C00"),
//            new TestElement(12, 2, "1652.00", "0x800000067400"),
//            new TestElement(12, 2, "17588.70", "0x80000044B446"),
//            new TestElement(12, 2, "8542.35", "0x800000215E23"),
//            new TestElement(12, 2, "12703.18", "0x800000319F12"),
//            new TestElement(12, 2, "6009.00", "0x800000177900"),
//            new TestElement(12, 2, "18850.68", "0x80000049A244"),
//            new TestElement(12, 2, "6436.92", "0x80000019245C"),
//
//            // The only value that was in the c_ytd_payment decimal(12,2) field
//            new TestElement(12, 2, "10.00", "0x800000000A00"),
//
//            // These next two aren't part of the bug, but we have them here anyway.
//            // One is an example from mysql docs, and the other caused us a problem before.
//            new TestElement(14, 4, "1234567890.1234", "0x810DFB38D204D2"),
//            new TestElement(12, 10, "90.1956251262", "0xDA0BA900A602")
//        );
//
//        for (TestElement test : tests) {
//            doTest(test.toString(), test.asNumber, test.precision, test.scale, test.asHex);
//        }
//    }
//
////    private static void doTest(String label, String expected, int precision, int scale, String bytesHex) {
////        byte[] bytes = bytes(bytesHex);
////        StringBuilder sb = new StringBuilder();
////        ConversionH
////        new FromObjectConversionSource().setReflectively()
////        DecimalEncoder.decodeToString(bytes, 0, precision, scale, AkibanAppender.of(sb));
////        BigDecimal actual = new BigDecimal(sb.toString());
////
////        assertEquals(label + ": BigDecimal", new BigDecimal(expected).toPlainString(), actual.toPlainString());
////        assertEquals(label + ": String", expected, sb.toString());
////    }
//
//    private static byte[]
//
//
//    @Test
//    public void testZeroValues() {
//        doTest("0 in (1,0)", "0", 1, 0, "0x80");
//        doTest("0 in (4,0)", "0", 4, 0, "0x8000");
//        doTest("0 in (8,0)", "0", 8, 0, "0x80000000");
//        doTest("0 in (30,0)", "0", 30, 0, "0x8000000000000000000000000000");
//        doTest("0 in (8,1)", "0.0", 8, 1, "0x8000000000");
//        doTest("0 in (8,2)", "0.00", 8, 2, "0x80000000");
//        doTest("0 in (10,4)", "0.0000", 10, 4, "0x8000000000");
//        doTest("0 in (30,8)", "0.00000000", 10, 8, "0x8000000000000000000000000000");
//    }
//}
