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
  
    @Test (expected = UnsupportedOperationException.class)
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
        Expression left = new LiteralExpression (AkType.YEAR,2006);
        Expression right = new LiteralExpression (AkType.YEAR,1991);
        Expression top = new ArithExpression (left, ex = ArithOps.MINUS, right);

        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.INTERVAL, 15L); // 15 years
        assertEquals("actuall = expected", expected, actual);
    }


    @Test
    public void dateMinusDate ()
    {
        Expression left = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"));
        Expression right = new LiteralExpression (AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-10-07"));
        Expression top = new ArithExpression (left, ex = ArithOps.MINUS, right);

        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.INTERVAL, 32L); // 32 days
        assertEquals("actuall = expected", expected, actual);
    }

    @Test
    public void timePlusInterval ()
    {
        Expression left = new LiteralExpression (AkType.TIME, 45L);
        Expression right = new LiteralExpression (AkType.INTERVAL, 10L);
        Expression top = new ArithExpression (left, ex = ArithOps.ADD, right);

        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder (top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.TIME, 55L);
        assertEquals("actual == expected", expected, actual);
    }

    @Test
    public void testTimeMinusTime ()
    {
        Expression left = new LiteralExpression (AkType.TIME, Extractors.getLongExtractor(AkType.TIME).getLong("12:30:25"));
        Expression right = new LiteralExpression (AkType.TIME, Extractors.getLongExtractor(AkType.TIME).getLong("12:30:30"));
        Expression top = new ArithExpression(right, ex = ArithOps.MINUS, left);

        ValueSource actual = new ValueHolder(top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.INTERVAL, 5L); // 5 secs
        assertEquals("actuall == expected", expected, actual);
    }

    @Test (expected = OverflowException.class)      
    public void bigIntPlusDouble () // expect exception
    {
        Expression left = new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(Long.MAX_VALUE).pow(100));
       
        Expression right = new LiteralExpression (AkType.DOUBLE, 100000000.0);
        Expression top = new ArithExpression(left, ex = ArithOps.ADD, right);
     
        ValueSource actual = new ValueHolder(top.evaluation().eval());        
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
}
