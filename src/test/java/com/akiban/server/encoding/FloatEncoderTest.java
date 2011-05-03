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

public final class FloatEncoderTest {
    static private class TestElement {
        private final float flt;
        private final String str;
        private final int intBits;

        public TestElement(float flt, int bits) {
            this.flt = flt;
            this.str = Float.valueOf(flt).toString();
            this.intBits = bits;
        }
        
        @Override
        public String toString() {
            return String.format("(%s, %f, %d)", str, flt, intBits);
        }
    }

    static private final float EPSILON = 0;

    private final TestElement[] TEST_CASES = {
            new TestElement(                -0.0f, 0x80000000),
            new TestElement(                 0.0f, 0x00000000),
            new TestElement(                -1.0f, 0xBF800000),
            new TestElement(                 1.0f, 0x3F800000),
            new TestElement(         8374.383789f, 0x4602D989),
            new TestElement(       -392749252608f, 0xD2B6E35C),
            new TestElement(             3.14159f, 0x40490FD0),
            new TestElement(        -6284.873535f, 0xC5C466FD),
            new TestElement(-6261550494905270272f, 0xDEADCAFE),
            new TestElement(         1337.003418f, 0x44A7201C),
            new TestElement(           275826464f, 0x4D838639),
            new TestElement(            16777216f, 0x4B800000)
    };


    @Test
    public void encodeToBits() {
        for(TestElement t : TEST_CASES) {
            final int bitsFromFloat = FloatEncoder.encodeFromObject(t.flt);
            final int bitsFromString = FloatEncoder.encodeFromObject(t.str);
            assertEquals("float->bits: " + t, t.intBits, bitsFromFloat);
            assertEquals("string->bits: " + t, t.intBits, bitsFromString);
        }
    }

    @Test
    public void decodeToFloat() {
        for(TestElement t : TEST_CASES) {
            final float decodeFromBits = FloatEncoder.decodeFromBits(t.intBits);
            assertEquals("bits->float: " + t, t.flt, decodeFromBits, EPSILON);
        }
    }

    @Test
    public void nullIsZero() {
        assertEquals("null not encoded to 0", 0, FloatEncoder.encodeFromObject(null), EPSILON);
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalidNumber() {
        FloatEncoder.encodeFromObject("zebra");
    }
}
