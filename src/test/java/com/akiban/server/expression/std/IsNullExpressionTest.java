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

import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;

import com.akiban.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class IsNullExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.BOOL, false);

    @Test
    public void testNullExpression ()
    {
        Expression  expression = new IsNullExpression(new LiteralExpression(AkType.NULL, null));
        ValueSource source = expression.evaluation().eval();
        assertTrue (source.getBool());
    }
    
    @Test 
    public void testNotnull ()
    {
        Expression expression = new IsNullExpression (new LiteralExpression(AkType.DOUBLE, 1.0));
        ValueSource source = expression.evaluation().eval();
        assertFalse(source.getBool());
    }   
 
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return IsNullExpression.COMPOSER;
    }
    
}
