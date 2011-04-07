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

public class DateEncoderTest {
    class TestElement {
        private final int asInt;
        private final Object asObject;
        private final String asString;

        public TestElement(int asInt, String asString, Object asObject) {
            this.asInt = asInt;
            this.asObject = asObject;
            this.asString = asString;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s, %s)", asInt, asString, asObject);
        }
    }

    final TestElement[] TEST_CASES = {
            // null conversion
            new TestElement(0, "0000-00-00", null),
            // Zero dates
            new TestElement(0, "0000-00-00", 0),
            new TestElement(31, "0000-00-31", 31),
            new TestElement(63, "0000-01-31", 63),
            // Valid dates
            new TestElement(1017180, "1986-10-28", 1017180),
            new TestElement(1029767, "2011-04-07", 1029767)
    };

    
    @Test
    public void fromObjects() {
        for(TestElement t : TEST_CASES) {
            assertEquals("Integer->int: " + t.toString(), 
                         t.asInt,
                         DateEncoder.objectToDateInt(t.asObject));
            assertEquals("String->int: " + t.toString(),
                         t.asInt,
                         DateEncoder.objectToDateInt(t.asString));
        }
    }

    @Test
    public void intToString() {
        for(TestElement t : TEST_CASES) {
            assertEquals("int->String: " + t.toString(),
                         t.asString,
                         DateEncoder.dateIntToString(t.asInt));
        }
    }
}