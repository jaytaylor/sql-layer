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

package com.akiban.server.expression.std;

import com.akiban.server.error.InvalidIntervalFormatException;
import com.akiban.server.error.ParseException;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.IntervalCastExpression.EndPoint;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.types.AkType.*;
import static com.akiban.server.expression.std.IntervalCastExpression.EndPoint.*;
public class IntervalCastExpressionTest 
{
    @Test
    public void testRegularCases()
    {  
        // year - month intervals
        test("123-34", YEAR_MONTH, INTERVAL_MONTH, 1510L);
        test("123", EndPoint.YEAR, INTERVAL_MONTH, 1476L);
        test(123, EndPoint.YEAR, INTERVAL_MONTH, 1476L);
        test("12", MONTH, INTERVAL_MONTH, 12L);

        // day intervals
        test("1", DAY, INTERVAL_MILLIS, 86400000L);
        test(1, DAY, INTERVAL_MILLIS, 86400000L);
        
        // day - sec intervals
        test("0 0:0:0.123", DAY_SECOND, INTERVAL_MILLIS, 123L);
        test("0            0:1:0.123", DAY_SECOND, INTERVAL_MILLIS, 60123L);

        // day - min intervals
        test("0 0:1", DAY_MINUTE, INTERVAL_MILLIS, 60000L);
        test("0 1:1", DAY_MINUTE, INTERVAL_MILLIS, 3660000L);

        // day - hr intervals
        test("0 0", DAY_HOUR, INTERVAL_MILLIS, 0L);
        test("1 0", DAY_HOUR, INTERVAL_MILLIS,86400000L);

        // hr -min
        test("0:1", HOUR_MINUTE, INTERVAL_MILLIS, 60000L);
        test("1:1", HOUR_MINUTE, INTERVAL_MILLIS, 3660000L);

        // hr - sec
        test("0:10:10.45", HOUR_SECOND, INTERVAL_MILLIS, 610450L);
        test("0:10:10.", HOUR_SECOND, INTERVAL_MILLIS, 610000L);

        // min - sec
        test("2:1", MINUTE_SECOND, INTERVAL_MILLIS, 121000L);
        test("2:1.12356", MINUTE_SECOND, INTERVAL_MILLIS, 121124L);

        // sec
        test("   1.123", SECOND, INTERVAL_MILLIS, 1123L);
        test("1.05", SECOND, INTERVAL_MILLIS, 1050L);
        test(25, SECOND, INTERVAL_MILLIS, 25000L);
        
        // min
        test("    20           ", MINUTE, INTERVAL_MILLIS, 1200000L);
        test("2", MINUTE, INTERVAL_MILLIS, 120000L);

        // hour
        test("     10         ", HOUR, INTERVAL_MILLIS, 36000000L);
        test("1    ", HOUR, INTERVAL_MILLIS, 3600000L);
    }
    
    @Test(expected = InvalidIntervalFormatException.class)
    public void testInvalidFormatYear ()
    {
        test("abc", YEAR_MONTH, INTERVAL_MONTH, 0); 
        
    }
    
    @Test(expected = InvalidIntervalFormatException.class)
    public void testMissingFields ()
    {
        test("12 ", YEAR_MONTH, INTERVAL_MONTH, 0);
    }
    
    @Test(expected = InvalidIntervalFormatException.class)
    public void testRedundantFields ()
    {
        test("12 2:12", YEAR_MONTH, INTERVAL_MONTH, 0);
    }
    
    private static void test(String str, EndPoint endPoint, AkType expType, long exp)
    {
        testAndCheck(new LiteralExpression(AkType.VARCHAR, str), endPoint, expType,exp);
    }

    private static void test(long lValue, EndPoint endPoint, AkType expType, long exp)
    {
        testAndCheck(new LiteralExpression(AkType.LONG, lValue), endPoint, expType, exp);
    }

    private static void testAndCheck (Expression input, EndPoint endPoint, AkType expType, long exp)
    {
        Expression interval = new IntervalCastExpression(input, endPoint);
        ValueSource source = interval.evaluation().eval();

        assertEquals("Assert INTERVAL type: ", expType, interval.valueType());
        assertEquals("Assert INTERVAL value: ", exp,
                expType == INTERVAL_MONTH ? source.getInterval_Month() : source.getInterval_Millis());
    }
}
