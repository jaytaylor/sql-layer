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
    public void dateMinusDate ()
    {
        Expression left = new LiteralExpression (AkType.DATE, 45L);
        Expression right = new LiteralExpression (AkType.DATE, 10L);
        Expression top = new ArithExpression (left, ex = ArithOps.MINUS, right);

        assertTrue("Top is const", top.isConstant());
        ValueSource actual = new ValueHolder (top.evaluation().eval());
        ValueSource expected = new ValueHolder(AkType.INTERVAL, 35L);
        assertEquals("actual == expected", expected, actual);
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
    protected int childrenCount() 
    {
        return 2;
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return (ExpressionComposer)ex;
    }
}
