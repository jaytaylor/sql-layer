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

import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import org.junit.runner.RunWith;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class TypeDeterminationTest // extends ComposedExpressionTestBase
{
    private AkType input1;
    private AkType input2;
    private AkType expected;
    private ArithOp op;
    
    public TypeDeterminationTest (AkType input1, ArithOp op,  AkType input2, AkType expected)
    {
        this.input1 = input1;
        this.input2 = input2;
        this.op = op;
        this.expected = expected;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // decimal
        param(pb, AkType.DECIMAL, ArithOps.ADD, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.U_BIGINT, ArithOps.DIVIDE, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.DOUBLE, ArithOps.MINUS, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.LONG, ArithOps.MULTIPLY, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.DECIMAL, ArithOps.ADD, AkType.VARCHAR, AkType.DECIMAL);
        
        // double
        param (pb, AkType.DOUBLE, ArithOps.MULTIPLY, AkType.U_BIGINT, AkType.DOUBLE);
        param(pb, AkType.LONG, ArithOps.ADD, AkType.DOUBLE, AkType.DOUBLE);
        param(pb, AkType.DOUBLE, ArithOps.DIVIDE, AkType.DOUBLE, AkType.DOUBLE);
        
        // u_bigint
        param(pb, AkType.U_BIGINT, ArithOps.MINUS, AkType.U_BIGINT, AkType.U_BIGINT);
        param(pb, AkType.LONG, ArithOps.MULTIPLY, AkType.U_BIGINT, AkType.U_BIGINT);
        
        //long
        param(pb, AkType.LONG, ArithOps.ADD, AkType.LONG, AkType.LONG);

        // date
        param(pb, AkType.DATE, ArithOps.MINUS, AkType.DATE, AkType.INTERVAL);
        param(pb, AkType.DATE, ArithOps.MINUS, AkType.INTERVAL, AkType.DATE);
        param(pb, AkType.DATE, ArithOps.ADD, AkType.INTERVAL, AkType.DATE);
        param(pb, AkType.INTERVAL, ArithOps.ADD, AkType.DATE, AkType.DATE);
        param(pb, AkType.INTERVAL, ArithOps.MINUS, AkType.DATE, null); // expect exception
        param(pb, AkType.DATE, ArithOps.DIVIDE, AkType.INTERVAL, null); // expect exception
        param(pb, AkType.DATE, ArithOps.ADD, AkType.DATE, null); // expect exception

        // time
        param(pb, AkType.TIME, ArithOps.MINUS, AkType.TIME, AkType.INTERVAL);
        param(pb, AkType.TIME, ArithOps.ADD, AkType.INTERVAL, AkType.TIME);
        param(pb, AkType.TIME, ArithOps.MINUS, AkType.INTERVAL, AkType.TIME);
        param(pb, AkType.INTERVAL, ArithOps.ADD, AkType.TIME, AkType.TIME);
        param(pb, AkType.INTERVAL, ArithOps.MINUS, AkType.TIME, null); // expect exception

        // year
        param(pb, AkType.YEAR, ArithOps.MINUS, AkType.YEAR, AkType.INTERVAL);
        param(pb, AkType.YEAR, ArithOps.ADD, AkType.INTERVAL, AkType.YEAR);
        param(pb, AkType.YEAR, ArithOps.MINUS, AkType.INTERVAL, AkType.YEAR);
        param(pb, AkType.INTERVAL, ArithOps.ADD, AkType.YEAR, AkType.YEAR);

        // INTERVAL
        param(pb, AkType.INTERVAL, ArithOps.ADD, AkType.INTERVAL, AkType.INTERVAL);
        param(pb, AkType.INTERVAL, ArithOps.MINUS, AkType.INTERVAL, AkType.INTERVAL);
        param(pb, AkType.INTERVAL, ArithOps.MULTIPLY, AkType.LONG, AkType.INTERVAL);
        param(pb, AkType.DECIMAL, ArithOps.MULTIPLY, AkType.INTERVAL, AkType.INTERVAL);
        param(pb, AkType.INTERVAL, ArithOps.DIVIDE, AkType.U_BIGINT, AkType.INTERVAL);
        param(pb, AkType.LONG, ArithOps.DIVIDE, AkType.INTERVAL, null); // expect exception
        param(pb, AkType.INTERVAL, ArithOps.DIVIDE, AkType.INTERVAL, null); // expect exception
        param(pb, AkType.INTERVAL, ArithOps.MULTIPLY, AkType.INTERVAL, null); // expect exception

        // exceptions
        param(pb, AkType.DATE, ArithOps.MINUS, AkType.TIME, null);
        param(pb, AkType.LONG, ArithOps.ADD, AkType.DATE, null);
        param(pb, AkType.YEAR, ArithOps.MULTIPLY, AkType.LONG, null);

        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, AkType input1, ArithOp op, AkType input2, AkType expected)
    {
        pb.add(input1.name() +  op.opName() + input2.name(), input1, op, input2, expected);       
    }

    @OnlyIfNot("exceptionExpected()")
    @Test
    public void test ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, op, right);               
        top.evaluation().eval();
        assertTrue(top.topT == expected);
    }
    
    @OnlyIf("exceptionExpected()")
    @Test (expected = UnsupportedOperationException.class)
    public void testWithException ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, op, right);
    }
    
    private Expression getExp (AkType type)
    {
        switch (type)
        {
            case LONG: return new LiteralExpression(type, 1L);
            case DOUBLE: return new LiteralExpression(type, 1.0);
            case DECIMAL: return new LiteralExpression(type, BigDecimal.ONE);
            case U_BIGINT: return new LiteralExpression(type, BigInteger.ONE);
            case DATE: return new LiteralExpression(type, 1L);
            case TIME: return new LiteralExpression(type, 1L);
            case INTERVAL: return new LiteralExpression(type, 1L);
            case YEAR: return new LiteralExpression(type, 1L);
            case VARCHAR: return new LiteralExpression (type, "1");
            case NULL: return new LiteralExpression(type, null);
            default: return new LiteralExpression(AkType.UNSUPPORTED, null);
        }
    }
    
    public boolean exceptionExpected ()
    {
        return expected == null;
    }
      
   // @Override
    protected ExpressionComposer getComposer() 
    {
        return (ExpressionComposer)op;
    }
    
}
