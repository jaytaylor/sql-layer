/**
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

import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;

public class LeftExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void testShortLenth()
    {
        test("abc", 2, "a");
        test("abc", 0, "");
    }
    
    private static void test(String st, int len, String expected)
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = new LiteralExpression(AkType.LONG, len);
        
        Expression top = new LeftExpression(str, length);
        
        assertEquals("LEFT(" + st + ", " + len + ") ", 
                    expected == null? NullValueSource.only() : new ValueHolder(AkType.VARCHAR, expected),
                    top.evaluation().eval());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return LeftExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
