/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        List<Expression> argList = new ArrayList<>();

        argList.add(new LiteralExpression(AkType.DOUBLE, 123.0));
        argList.add(new LiteralExpression(AkType.DOUBLE, 34.3));

        Expression top = compose(DegreesExpression.COMPOSER, argList);
        top.evaluation().eval();
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testNoArgs()
    {
        List<Expression> argList = new ArrayList<>();
        
        Expression top = compose(DegreesExpression.COMPOSER, argList);
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
