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

import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.std.BinaryBitExpression.BitOperator;
import com.akiban.server.types.AkType;

import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class BinaryBitExpressionTest 
{
    static interface Functor
    {
         BigInteger calc (BigInteger l, BigInteger r);
    }

    private static final Functor functor[] = new Functor[]
    {
      new Functor() { public BigInteger calc (BigInteger l, BigInteger r) {return l.and(r);}},
      new Functor() { public BigInteger calc (BigInteger l, BigInteger r) {return l.or(r);}},
      new Functor() { public BigInteger calc (BigInteger l, BigInteger r) {return l.xor(r);}},
      new Functor() { public BigInteger calc (BigInteger l, BigInteger r) {return l.shiftLeft(r.intValue());}},
      new Functor() { public BigInteger calc (BigInteger l, BigInteger r) {return l.shiftRight(r.intValue());}}
    };

    private ExpressionComposer composer = BinaryBitExpression.B_AND_COMPOSER;
    private BitOperator op = BitOperator.BITWISE_AND;

    public BinaryBitExpressionTest (ExpressionComposer composer, BitOperator op)
    {
        this.composer = composer;
        this.op = op;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        param(pb, BinaryBitExpression.B_AND_COMPOSER, BitOperator.BITWISE_AND);
        param(pb, BinaryBitExpression.B_OR_COMPOSER, BitOperator.BITWISE_OR);
        param(pb, BinaryBitExpression.B_XOR_COMPOSER, BitOperator.BITWISE_XOR);
        param(pb, BinaryBitExpression.LEFT_SHIFT_COMPOSER, BitOperator.LEFT_SHIFT);
        param(pb, BinaryBitExpression.RIGHT_SHIFT_COMPOSER, BitOperator.RIGHT_SHIFT);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, ExpressionComposer c, BitOperator op)
    {
        pb.add(op.name(), c, op);
    }

    @Test
    public void testLongWithUBigint ()
    {
        Expression leftEx = new LiteralExpression(AkType.U_BIGINT,BigInteger.valueOf(10));
        Expression rightEx = new LiteralExpression(AkType.LONG, 10);
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(10), BigInteger.valueOf(10)), getActual(leftEx, rightEx));
    }

    @Test
    public void testStringWithString ()
    {
        Expression leftEx = new LiteralExpression(AkType.VARCHAR, "2");                
        assertEquals( functor[op.ordinal()].calc(BigInteger.valueOf(2), BigInteger.valueOf(2)), getActual(leftEx, leftEx));
    }

    @Test
    public void testStringWithLong()
    {
        Expression leftEx = new LiteralExpression(AkType.VARCHAR, "2");
        Expression rightEx =  new LiteralExpression(AkType.LONG, 5);
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(2), BigInteger.valueOf(5)), getActual(leftEx,rightEx));
    }

    @Test  
    public void testStringWithDouble() 
    {
        Expression leftEx = new LiteralExpression(AkType.VARCHAR, "2");
        Expression rightEx = new LiteralExpression(AkType.DOUBLE, 3.5);
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(2), BigInteger.valueOf(4)), getActual(leftEx,rightEx));
    }

    @Test
    public void testNonNumeric()
    {
        Expression leftEx = new LiteralExpression(AkType.VARCHAR, "a");
        Expression rightEx = new LiteralExpression(AkType.VARCHAR, "b");
        assertEquals(BigInteger.valueOf(0), getActual(leftEx, rightEx));
    }

    @Test
    public void testNullwithLong()
    {
        Expression leftEx = new LiteralExpression(AkType.NULL, null);
        Expression rightEx = new LiteralExpression(AkType.LONG, 2L);

        ValueSource actual = composer.compose(Arrays.asList(leftEx, rightEx)).evaluation().eval();        
        assertEquals(NullValueSource.only(), actual);
    }

    @Test
    public void testNullWithNull()
    {
        Expression nullEx = LiteralExpression.forNull();
        ValueSource actual = composer.compose(Arrays.asList(nullEx, nullEx)).evaluation().eval();        
        assertEquals(NullValueSource.only(), actual);
    }

    private BigInteger getActual (Expression left, Expression right)
    {
        return composer.compose(Arrays.asList(left, right)).evaluation().eval().getUBigInt();
    } 
}
