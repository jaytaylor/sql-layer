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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class LeftExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        // test shorter length
        test("abc", 2, "ab");
        test("abc", 0, "");
        test("abc", -4, "");
        
        // test longer length
        test("abc", 4, "abc");
        test("abc", 3, "abc");
        
        // test null
        test(null, 3, null);
        test("ab", null, null);
        test(null, null, null);
        
        // test wrong arity
        testWrongArity(0);
        testWrongArity(1);
        testWrongArity(3);
        testWrongArity(4);
    }    
    private static void testWrongArity(int argc)
    {
        try
        {
            List<Expression> args = new ArrayList<Expression>();
            for (int n = 0; n < argc; ++n)
                args.add(LiteralExpression.forNull());
            LeftExpression.COMPOSER.compose(args);
        }
        catch (WrongExpressionArityException ex)
        {
            return;
        }
        
        assert false : "Shouldn't have made it here!";
    }
    
    private static void test(String st, Integer len, String expected)
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = len == null? LiteralExpression.forNull():
                            new LiteralExpression(AkType.LONG,  len.intValue());
        
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
