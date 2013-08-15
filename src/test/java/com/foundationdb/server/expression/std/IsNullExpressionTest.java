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


package com.foundationdb.server.expression.std;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;

import com.foundationdb.server.types.ValueSource;
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

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
