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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.std.BinaryBitwiseOpExpression.BitOperator;
import java.math.BigInteger;
import org.junit.runner.RunWith;

@RunWith(NamedParameterizedRunner.class)
public class BinaryBitwiseOpExpressionTest extends ComposedExpressionTestBase
{
    private BigInteger left;
    private BigInteger right;
    private BigInteger expected;
    private BitOperator op; 
    private boolean expectExc;
    private ExpressionComposer composer = BinaryBitwiseOpExpression.B_AND_COMPOSER;
    
    public BinaryBitwiseOpExpressionTest (BigInteger left, BigInteger right, 
            BigInteger expected, BitOperator op, boolean expectExc)
    {
        this.left = left;
        this.right = right;
        this.expected = expected;
        this.op = op;
        this.expectExc = expectExc;
    }
     
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        BigInteger l = BigInteger.valueOf(12);
        BigInteger r = BigInteger.valueOf(10);
        
        param(pb, l, r, l.and(r),BitOperator.BITWISE_AND, false);
    }
    
    private static void param (ParameterizationBuilder pb, BigInteger left,BigInteger right,
            BigInteger expected, BitOperator op, boolean exp)
    {
        pb.add(left + " " + op + " " + right, left, right, expected, op, exp);
    }
    
    @Override
    protected int childrenCount() 
    {
        return 2;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
