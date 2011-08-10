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
import java.math.BigInteger;

public class DateTimeConverterTest extends LongConverterTestBase {
    public DateTimeConverterTest() {
        super(ConvertersForDates.DATETIME,
              new TestElement[] {
                new TestElement("0000-00-00 00:00:00", 0),
                new TestElement("1000-01-01 00:00:00", 10000101000000L),
                new TestElement("9999-12-31 23:59:59", 99991231235959L),
                new TestElement("2011-04-10 17:04:03", Long.valueOf(20110410170403L)),
                new TestElement("1986-10-28 05:20:00", BigInteger.valueOf(19861028052000L))
              });
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
