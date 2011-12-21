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

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.error.OverflowException;
import com.akiban.server.error.DivisionByZeroException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArithExpressionTest  extends ComposedExpressionTestBase
{
    protected ArithOp ex =  ArithOps.MINUS;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.DOUBLE, true);

    @Test
    public void longMinusDouble ()
    {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = new LiteralExpression(AkType.DOUBLE, 2.0);
        Expression top = new ArithExpression(left, ex = ArithOps.MINUS, right);
   
        assertTrue("top should be constant", top.isConstant());
        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.DOUBLE, 3.0);
        assertEquals("ValueSource", expected, actual);
       
    }
    
    @Test
    public void bigDecimalTimesBigInteger () 
    {
        Expression left = new LiteralExpression (AkType.DECIMAL, BigDecimal.valueOf(2.0));
        Expression right = new LiteralExpression(AkType.U_BIGINT, BigInteger.ONE);
        Expression top = new ArithExpression(left, ex = ArithOps.MULTIPLY, right);
        
        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.DECIMAL, BigDecimal.valueOf(2.0));
        assertEquals("actual == expected", expected, actual);
    }

    @Test
    public void longDivideLong ()
    {
        Expression left = new LiteralExpression (AkType.LONG, 2L);
        Expression right = new LiteralExpression(AkType.LONG, 5L);
        Expression top = new ArithExpression(left, ex = ArithOps.DIVIDE, right);
        
        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.LONG, 0L);
        assertEquals("actual == expected", expected, actual);
    }
    
    @Test
    public void bigIntPlusBigInt ()
    {
        Expression left = new LiteralExpression (AkType.U_BIGINT, BigInteger.ONE);
        Expression right = new LiteralExpression (AkType.U_BIGINT, BigInteger.TEN);
        Expression top = new ArithExpression (left, ex = ArithOps.ADD, right);
        
        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder (top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.U_BIGINT, BigInteger.valueOf(11L));
        assertEquals("actual == expected", expected, actual);
    }
    
    @Test (expected = DivisionByZeroException.class)
    public void divideByZero ()
    {
        Expression left = new LiteralExpression (AkType.U_BIGINT, BigInteger.ONE);
        Expression right = new LiteralExpression (AkType.U_BIGINT, BigInteger.ZERO);
        Expression top = new ArithExpression (left, ex = ArithOps.DIVIDE, right);
        ValueSource actual = new ValueHolder (top.evaluation().eval());
    }
  
    @Test (expected = InvalidArgumentTypeException.class)
    public void datePlusLong ()
    {
        Expression left = new LiteralExpression (AkType.DATE, 1L);
        Expression right = new LiteralExpression (AkType.LONG, 2L);
        Expression top = new ArithExpression (left, ex = ArithOps.ADD, right);
        
        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder (top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.LONG, 3L);
        assertEquals("actuall = expected", expected, actual);
    }
    
    @Test 
    public void longPlusString ()
    {
        Expression left = new LiteralExpression (AkType.U_BIGINT, BigInteger.ONE);
        Expression right = new LiteralExpression (AkType.VARCHAR,"2");
        Expression top = new ArithExpression (left, ex = ArithOps.ADD, right);
        
        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder (top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.U_BIGINT, BigInteger.valueOf(3L));
        assertEquals("actual == expected", expected, actual);
    }

    @Test
    public void yearMinusYear ()
    {
        Expression year1 = new LiteralExpression (AkType.YEAR,Extractors.getLongExtractor(AkType.YEAR).getLong("2006"));
        Expression year2 = new LiteralExpression (AkType.YEAR,Extractors.getLongExtractor(AkType.YEAR).getLong("1991"));
        Expression interval = new ArithExpression (year1, ex = ArithOps.MINUS, year2);

        Expression datetime = new LiteralExpression (AkType.DATETIME, 19940229123010L); // 1994-02-29 12:30:10
        Expression rst = new ArithExpression (datetime, ex = ArithOps.ADD, interval);

        // 2006 minus 1991 plus 1994-02-29 12:30:10 equals 2009-03-01 12:30:10L (1994 is a leap year, while 2009 is NOT)
        assertEquals(20090301123010L, rst.evaluation().eval().getDateTime());
    }


    @Test
    public void dateMinusDatePlusDate ()
    {
        Expression date1 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"));
        Expression date2 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-06"));
        Expression interval = new ArithExpression (date1, ex = ArithOps.MINUS, date2);
       
        Expression date08 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2008-02-28"));
        Expression rst1 = new ArithExpression(interval, ex = ArithOps.ADD, date08);

        Expression date09 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-02-28"));
        Expression rst2 = new ArithExpression(interval, ex = ArithOps.ADD, date09);

        // 2006-11-07 minus 2006-11-06 plus 2006-12-12 equals 2008-02-29 (2008 is a leap year)
        assertEquals("2008-02-29", Extractors.getLongExtractor(AkType.DATE).asString(rst1.evaluation().eval().getDate()));

        // 2006-11-07 minus 2006-11-06 plus 2006-12-12 equals 2009-03-01 (2009 is NOT a leap year)
        assertEquals("2009-03-01", Extractors.getLongExtractor(AkType.DATE).asString(rst2.evaluation().eval().getDate()));
    }

    @Test
    public void testDateMinusDatePlusTime ()
    {
        // date - date => interval
        Expression date1 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"));
        Expression date2 = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-10-07"));
        Expression interval = new ArithExpression(date1, ex = ArithOps.MINUS, date2);

        // time + interval => time
        Expression left = new LiteralExpression (AkType.TIME, 123010L);        
        Expression top = new ArithExpression (left, ex = ArithOps.ADD, interval);

        assertTrue("Top is const", top.isConstant());

        // 12:30:10 plus (2006-11-07 minus 2006-11-06) equals 12:30:10 => time doesn't change
        assertEquals(123010L, top.evaluation().eval().getTime());
    }

    @Test
    public void testTimeMinusTimePlusDatetime ()
    {
        Expression time1 = new LiteralExpression (AkType.TIME, Extractors.getLongExtractor(AkType.TIME).getLong("12:30:25"));
        Expression time2 = new LiteralExpression (AkType.TIME, Extractors.getLongExtractor(AkType.TIME).getLong("12:30:30"));
        Expression interval = new ArithExpression(time2, ex = ArithOps.MINUS, time1); // interval of 5 secs

        Expression datetime1 = new LiteralExpression (AkType.DATETIME, 20080228235956L); // 2008-02-28 23:59:56
        Expression datetime2 = new LiteralExpression (AkType.DATETIME, 20090228235956L); // 2009-02-28 23:59:56
        Expression rst1 = new ArithExpression(datetime1, ex = ArithOps.ADD, interval);
        Expression rst2 = new ArithExpression(datetime2, ex = ArithOps.ADD, interval);
                
        // 2008-02-28 23:59:56 plus 5 secs equals 2008-02-29 00:00:01  (2008 is a leap year)
        assertEquals(20080229000001L, rst1.evaluation().eval().getDateTime());

        // 2009-02-28 23:59:56 plus 5 secs equals 2009-03-01 00:00:01  (2009 is NOT a leap year)
        assertEquals(20090301000001L, rst2.evaluation().eval().getDateTime());
    }

    @Test (expected = OverflowException.class)      
    public void bigIntPlusDouble () // expect exception
    {
        Expression left = new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(Long.MAX_VALUE).pow(100));
       
        Expression right = new LiteralExpression (AkType.DOUBLE, 100000000.0);
        Expression top = new ArithExpression(left, ex = ArithOps.ADD, right);
     
        ValueSource actual = new ValueHolder(top.evaluation().eval());        
    }

    @Test
    public void testDatePlusIntervalMonth ()
    {
        Expression left = new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-02-23"));
        Expression right = new LiteralExpression(AkType.INTERVAL_MONTH, 23L);
        
        Expression top = new ArithExpression(left, ex = ArithOps.ADD, right);
        
        String actual = Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate());
        
        assertEquals("2011-01-23", actual);
    }
    
    @Override
    protected ExpressionComposer getComposer()
    {
        return (ExpressionComposer)ex;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    public boolean alreadyExc()
    {
        return false;
    }
}
