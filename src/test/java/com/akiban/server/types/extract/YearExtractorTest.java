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

package com.akiban.server.types.extract;

import org.junit.Assert;
import org.junit.Test;

import com.akiban.server.error.InvalidDateFormatException;

public class YearExtractorTest extends LongExtractorTestBase {
    public YearExtractorTest() {
        super(ExtractorsForDates.YEAR,
              new TestElement[] {
                new TestElement("0", 0),
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
        Assert.assertEquals("2", encodeAndDecode("2"));
        Assert.assertEquals("20", encodeAndDecode("20"));
        Assert.assertEquals("201", encodeAndDecode("201"));
        Assert.assertEquals("2011", encodeAndDecode("2011"));
    }

    @Test(expected=InvalidDateFormatException.class)
    public void invalidNumber() {
        encodeAndDecode("20111zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }
}
