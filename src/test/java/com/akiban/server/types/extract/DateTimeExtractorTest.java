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

import org.junit.Test;
import static org.junit.Assert.*;

import com.akiban.server.error.InvalidDateFormatException;

import java.math.BigInteger;

public class DateTimeExtractorTest extends LongExtractorTestBase {
    public DateTimeExtractorTest() {
        super(ExtractorsForDates.DATETIME,
              new TestElement[] {
                new TestElement("0000-00-00 00:00:00", 0),
                new TestElement("1000-01-01 00:00:00", 10000101000000L),
                new TestElement("9999-12-31 23:59:59", 99991231235959L),
                new TestElement("2011-04-10 17:04:03", Long.valueOf(20110410170403L)),
                new TestElement("1986-10-28 05:20:00", BigInteger.valueOf(19861028052000L))
              });
    }


    @Test(expected=InvalidDateFormatException.class)
    public void datePartNotNumber() {
        encodeAndDecode("2011-01-01zebra 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void timePartNotNumber() {
        encodeAndDecode("2011-01-01 00:00:00zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void partialDatePart() {
        encodeAndDecode("2011-01 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void partialTimePart() {
        encodeAndDecode("2011-01-01 00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void noNumbers() {
        encodeAndDecode("zebra");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyDateParts() {
        encodeAndDecode("2000-01-01-01 00:00:00");
    }

    @Test(expected=InvalidDateFormatException.class)
    public void tooManyTimeParts() {
        encodeAndDecode("2000-01-01 00:00:00:00");
    }

    // does not expect exception
    // missing time-fields will just be filled with 00:00:00
    @Test//(expected=InvalidDateFormatException.class)
    public void onlyDatePart() {
        encodeAndDecode("2000-01-01");
        long actual = ExtractorsForDates.DATETIME.getLong("2000-01-01");
        assertEquals(20000101000000L, actual);
    }

    @Test(expected=InvalidDateFormatException.class)
    public void onlyTimePart() {
        encodeAndDecode("10:11:12");
    }
}
