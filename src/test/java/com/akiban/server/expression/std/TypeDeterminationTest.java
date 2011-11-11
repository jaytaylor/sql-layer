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
public class TypeDeterminationTest 
{
    private AkType input1;
    private AkType input2;
    private AkType expected;
    
    public TypeDeterminationTest (AkType input1, AkType input2, AkType expected)
    {
        this.input1 = input1;
        this.input2 = input2;
        this.expected = expected;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        /*
         * basic idea: if any of the two operand is 
         *                   decimal =>  result = decimal
         *              else if it's
         *                   double => double
         *              else if it's
         *                    U_BIGINT => U_BIGINT
         *              else (long) = > long
         */
        
        // decimal
        param(pb, AkType.DECIMAL, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.U_BIGINT, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.DOUBLE, AkType.DECIMAL, AkType.DECIMAL);
        param(pb, AkType.LONG, AkType.DECIMAL, AkType.DECIMAL);
        
        // double
        param (pb, AkType.DOUBLE, AkType.U_BIGINT, AkType.DOUBLE);
        param(pb, AkType.LONG, AkType.DOUBLE, AkType.DOUBLE);
        param(pb, AkType.DOUBLE, AkType.DOUBLE, AkType.DOUBLE);
        
        // u_bigint
        param(pb, AkType.U_BIGINT, AkType.U_BIGINT, AkType.U_BIGINT);
        param(pb, AkType.LONG, AkType.U_BIGINT, AkType.U_BIGINT);
        
        //long
        param(pb, AkType.LONG, AkType.LONG, AkType.LONG);
        
        // exeptions
        param(pb, AkType.DATE, AkType.TIME, null);
        param(pb, AkType.DOUBLE, AkType.DATE, null);
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, AkType input1, AkType input2,
            AkType expected)
    {
        pb.add(input1.name() + " AND " + input2.name(), input1, input2, expected);
        pb.add(input2.name() + " AND " + input1.name() + "(2)", input2, input1, expected); // just to document the symetry
    }

    @OnlyIfNot("exceptionExpected()")
    @Test
    public void test ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, ArithOps.ADD, right);
       
        assertTrue(top.topT == expected); 
    }
    
    @OnlyIf("exceptionExpected()")
    @Test (expected = UnsupportedOperationException.class)
    public void testWithException ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, ArithOps.ADD, right);
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
            case NULL: return new LiteralExpression(type, null);
            default: return new LiteralExpression(AkType.UNSUPPORTED, null);
        }
    }
    
    public boolean exceptionExpected ()
    {
        return expected == null;
    }    
}
