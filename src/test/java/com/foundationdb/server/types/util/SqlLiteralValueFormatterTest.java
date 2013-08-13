/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.util;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ValueSource;

import org.junit.Test;
import static org.junit.Assert.*;

public class SqlLiteralValueFormatterTest
{
    @Test
    public void testDate() throws Exception {
        test("DATE '2001-01-01'", "2001-01-01", AkType.DATE);
    }

    @Test
    public void testDatetime() throws Exception {
        test("DATETIME '2001-01-01 00:00:00'", "2001-01-01", AkType.DATETIME);
        test("DATETIME '2001-01-01 13:01:59'", "2001-01-01 13:01:59", AkType.DATETIME);
    }

    @Test
    public void testDecimal() throws Exception {
        test("3.14", "3.14", AkType.DECIMAL);
    }

    @Test
    public void testDouble() throws Exception {
        test("3.140000e+00", 3.14);
    }

    @Test
    public void testFloat() throws Exception {
        test("3.140000e+00", 3.14f);
    }

    @Test
    public void testInt() throws Exception {
        test("1", 1);
        test("-1", -1);
    }

    @Test
    public void testLong() throws Exception {
        test("2", 2L);
        test("-2", -2L);
    }

    @Test
    public void testVarchar() throws Exception {
        test("'abc'", "abc");
        test("'ab''c'", "ab'c");
    }

    @Test
    public void testText() throws Exception {
        test("'abc'", "abc", AkType.TEXT);
    }

    @Test
    public void testTime() throws Exception {
        test("TIME '23:59:59'", "23:59:59", AkType.TIME);
    }

    @Test
    public void testTimestamp() throws Exception {
        test("TIMESTAMP '2001-01-01 13:01:59'", "2001-01-01 13:01:59", AkType.TIMESTAMP);
    }

    @Test
    public void testUBigint() throws Exception {
        test("9223372036854775808", "9223372036854775808", AkType.U_BIGINT);
    }

    @Test
    public void testUDouble() throws Exception {
        test("3.140000e+00", 3.14, AkType.U_DOUBLE);
    }

    @Test
    public void testUFloat() throws Exception {
        test("3.140000e+00", 3.14f, AkType.U_FLOAT);
    }

    @Test
    public void testUInt() throws Exception {
        test("2147483648", 0x80000000L, AkType.U_INT);
    }

    @Test
    public void testVarbinary() throws Exception {
        test("X'01020304'", new byte[] { 1,2,3,4 });
        test("X'FF884422'", new byte[] { (byte)0xFF,(byte)0x88,(byte)0x44,(byte)0x22 });
    }

    @Test
    public void testYear() throws Exception {
        test("1969", 1969, AkType.YEAR);
    }

    @Test
    public void testBool() throws Exception {
        test("TRUE", true);
        test("FALSE", false);
    }

    @Test
    public void testIntervalMillis() throws Exception {
        test("INTERVAL '30' DAY", 30 * 24 * 60 * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '10' HOUR", 10 * 60 * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '15' MINUTE", 15 * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '59' SECOND", 59 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '10.100' SECOND", 10100L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '1:12' DAY TO HOUR", (24 + 12) * 60 * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '2:00:30' DAY TO MINUTE", (2 * 24 * 60 + 30) * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '3:10:05:50' DAY TO SECOND", (((3 * 24 + 10) * 60 + 5) * 60 + 50) * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '3:10:05:00.999' DAY TO SECOND", ((3 * 24 + 10) * 60 + 5) * 60 * 1000L + 999, AkType.INTERVAL_MILLIS);
        test("INTERVAL '2:30' HOUR TO MINUTE", (2 * 60 + 30) * 60 * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '10:05:50' HOUR TO SECOND", ((10 * 60 + 5) * 60 + 50) * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '10:05:00.999' HOUR TO SECOND", (10 * 60 + 5) * 60 * 1000L + 999, AkType.INTERVAL_MILLIS);
        test("INTERVAL '7:50' MINUTE TO SECOND", (7 * 60 + 50) * 1000L, AkType.INTERVAL_MILLIS);
        test("INTERVAL '7:00.999' MINUTE TO SECOND", 7 * 60 * 1000L + 999, AkType.INTERVAL_MILLIS);
        test("INTERVAL '-3:10:05:00.999' DAY TO SECOND", - (((3 * 24 + 10) * 60 + 5) * 60 * 1000L + 999), AkType.INTERVAL_MILLIS);
    }

    @Test
    public void testIntervalMonth() throws Exception {
        test("INTERVAL '3' MONTH", 3, AkType.INTERVAL_MONTH);
        test("INTERVAL '3' YEAR", 36, AkType.INTERVAL_MONTH);
        test("INTERVAL '3-06' YEAR TO MONTH", 42, AkType.INTERVAL_MONTH);
    }

    @Test
    public void testNull() throws Exception {
        test("NULL", (String)null);
    }

    private FromObjectValueSource valueSource = new FromObjectValueSource();

    protected void test(String expected, Object object) throws Exception {
        valueSource.setReflectively(object);
        test(expected, valueSource);
    }

    protected void test(String expected, Object object, AkType type) throws Exception {
        valueSource.setExplicitly(object, type);
        test(expected, valueSource);
    }

    protected void test(String expected, ValueSource value) throws Exception {
        String sql = SqlLiteralValueFormatter.format(value);
        assertEquals(expected, sql);
    }
}
