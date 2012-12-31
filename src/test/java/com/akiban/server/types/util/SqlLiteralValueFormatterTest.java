/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types.util;

import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;

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
