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

package com.akiban.server.types.conversion;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class YearConverterTest extends LongConverterTestBase {
    public YearConverterTest() {
        super(ConvertersForDates.YEAR,
              new TestElement[] {
                new TestElement("0000", 0),
                new TestElement("1901", 1),
                new TestElement("1950", 50),
                new TestElement("2000", 100),
                new TestElement("2028", 128),
                new TestElement("2029", 129),
                new TestElement("2155", 255),
                new TestElement("2011", new Integer(111)),
                new TestElement("1986", new Long(86))
              });
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
