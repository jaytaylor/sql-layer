/*
 * Copyright (C) 2012 Akiban Technologies Inc.
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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import junit.framework.Assert;
import org.junit.Test;

public class SqrtExpressionTest extends ComposedExpressionTestBase {
    @Test
    public void testLiteral()
    {
        Expression wholeNumberLiteral = new SqrtExpression(ExprUtil.lit(16.0d));
        Expression fractionalLiteral = new SqrtExpression(ExprUtil.lit(13.33d));
        
        double sqrtWhole = wholeNumberLiteral.evaluation().eval().getDouble();
        double sqrtFrac = fractionalLiteral.evaluation().eval().getDouble();
        
        Assert.assertEquals(4.0d, sqrtWhole, 0.0001);
        Assert.assertEquals(3.65102d, sqrtFrac, 0.0001);
    }
    
    @Test
    public void testNegative()
    {
        Expression negLiteral = new SqrtExpression(ExprUtil.lit(-4.0d));
        ValueSource sqrtOfNegValueSource = negLiteral.evaluation().eval();

        Assert.assertTrue(sqrtOfNegValueSource.isNull());
    }
    
    @Test
    public void testInfinity()
    {
        Expression infinityLiteral = new SqrtExpression(ExprUtil.lit(Double.POSITIVE_INFINITY));
        double sqrtInfinity = infinityLiteral.evaluation().eval().getDouble();
        
        Assert.assertTrue(Double.isInfinite(sqrtInfinity));
    }

    @Test
    public void testNull()
    {
        Expression nullExpression = new SqrtExpression(ExprUtil.constNull());
        ValueSource sqrtOfNullExpression = nullExpression.evaluation().eval();
        
        Assert.assertEquals("SQRT(NULL) value source should be NullValueSource", 
                NullValueSource.only(), sqrtOfNullExpression);
        Assert.assertEquals("SQRT of NULL should be NULL", 
                AkType.NULL, sqrtOfNullExpression.getConversionType());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return SqrtExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
