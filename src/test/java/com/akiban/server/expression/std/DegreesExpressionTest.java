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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DegreesExpressionTest extends ComposedExpressionTestBase
{

    @Test(expected = WrongExpressionArityException.class)
    public void testIllegalArg()
    {
        List<Expression> argList = new ArrayList<Expression>();

        argList.add(new LiteralExpression(AkType.DOUBLE, 123.0));
        argList.add(new LiteralExpression(AkType.DOUBLE, 34.3));

        Expression top = DegreesExpression.COMPOSER.compose(argList);
        top.evaluation().eval();
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testNoArgs()
    {
        List<Expression> argList = new ArrayList<Expression>();
        
        Expression top = DegreesExpression.COMPOSER.compose(argList);
        top.evaluation().eval();
    }

    @Test
    public void testNull()
    {
        Expression arg = LiteralExpression.forNull();
        Expression top = new DegreesExpression(arg);

        Assert.assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void testRegular()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, Math.PI);
        Expression top = new DegreesExpression(arg);

        Assert.assertEquals(180.0, top.evaluation().eval().getDouble(), 0.0001);
    }

    @Test
    public void testNegative()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, -Math.PI);
        Expression top = new DegreesExpression(arg);

        Assert.assertEquals(-180.0, top.evaluation().eval().getDouble(), 0.0001);
    }

    @Test
    public void testInfinity()
    {
        Expression infinity = new LiteralExpression(AkType.DOUBLE, Double.POSITIVE_INFINITY);
        Expression top = new DegreesExpression(infinity);
        double degreesInfinity = top.evaluation().eval().getDouble();

        Assert.assertTrue(Double.isInfinite(degreesInfinity));
    }

    @Test
    public void testNegInfinity()
    {
        Expression negInfinity = new LiteralExpression(AkType.DOUBLE, Double.NEGATIVE_INFINITY);
        Expression top = new DegreesExpression(negInfinity);
        double degreesNegInfinity = top.evaluation().eval().getDouble();

        Assert.assertTrue(Double.isInfinite(degreesNegInfinity));
    }

    @Test
    public void testNaN()
    {
        Expression nan = new LiteralExpression(AkType.DOUBLE, Double.NaN);
        Expression top = new DegreesExpression(nan);
        double degreesNan = top.evaluation().eval().getDouble();

        Assert.assertTrue(Double.isNaN(degreesNan));
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return DegreesExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
