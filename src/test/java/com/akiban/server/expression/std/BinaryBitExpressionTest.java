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
