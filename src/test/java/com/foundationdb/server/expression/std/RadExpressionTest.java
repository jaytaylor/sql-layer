/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.expression.std;

import java.util.Arrays;
import com.foundationdb.server.error.WrongExpressionArityException;
import org.junit.Test;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;

import static org.junit.Assert.*;

public class RadExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        doTest(180.0, Math.PI);
        doTest(90.0, Math.PI/2);
        doTest(45.0, Math.PI/4);
        doTest(30.0, Math.PI/6);
        doTest(60.0, Math.PI/3);
        doTest(270.0, 3* Math.PI / 2);
        doTest(null, null);
    }
    
    @Test(expected=WrongExpressionArityException.class)
    public void testArity()
    {
        Expression top = compose(RandExpression.COMPOSER, Arrays.asList(LiteralExpression.forNull(), LiteralExpression.forNull()));
    }
    
    private static void doTest(Double input, Double expected)
    {
        Expression top = new RadExpression(input == null ? LiteralExpression.forNull()
                                                         :new LiteralExpression(AkType.DOUBLE, input.doubleValue()));
        String testName = "[RAD(" + input + ")]";
        
        if (expected == null)
            assertTrue(testName + " Top shoud be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(testName, expected.doubleValue(), top.evaluation().eval().getDouble(), 0.0001);
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return RadExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
