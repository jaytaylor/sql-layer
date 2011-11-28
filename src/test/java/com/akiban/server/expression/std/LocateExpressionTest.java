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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LocateExpressionTest extends ComposedExpressionTestBase 
{
    
    @Test
    public void test() 
    {
        //test 2 args
        testLocate("bar", "foobarbar", 4);
        testLocate("xbar", "foobarbar", 0);
        testLocate("", "bar", 1);
        testLocate("", "", 1);
        testLocate("a", "", 0);
        testLocate( null, "abc", 0);
        testLocate( "bar", null, 0);
        
        // test 3 args
        testLocate("bar", "foobarbar", 5, 7);
        testLocate("bar", "foobarbar", -5, 0);
        testLocate("", "foobarbar", 1,1);
        testLocate("", "", 1,1);
        testLocate("", "", 3, 0);
        testLocate(null, "abc", 3, 0);
        testLocate("abc", null, 3,0);
        
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testWithExc ()
    {
        Expression dummy = new LiteralExpression(AkType.VARCHAR, "abc");
        Expression top = new LocateExpression(Arrays.asList(dummy, dummy, dummy, dummy));
    }
    
    private static void testLocate(String substr, String str, int expected) 
    {
        boolean expectNull;
        Expression strEx = new LiteralExpression((expectNull = str == null) ? AkType.NULL : AkType.VARCHAR, str);
        Expression subEx = new LiteralExpression((expectNull |= substr == null) ? AkType.NULL : AkType.VARCHAR, substr);
        
        Expression top = new LocateExpression(Arrays.asList(subEx, strEx));
        
        if (expectNull) 
            assertTrue("expect locate (" + substr + ", " + str + ", " + "), expect null, but was not null", top.evaluation().eval().isNull());
        else 
        {
            long actual = top.evaluation().eval().getLong();
            assertTrue("expect locate (" + substr + ", " + str + ") = " + expected + " but was " + actual,
                    actual == expected);
        }
    }
    
    private static void testLocate(String substr, String str, int pos, long expected) 
    {
        boolean expectNull;
        Expression strEx = new LiteralExpression((expectNull = str == null) ? AkType.NULL : AkType.VARCHAR, str);
        Expression subEx = new LiteralExpression((expectNull |= substr == null) ? AkType.NULL : AkType.VARCHAR, substr);
        Expression posEx = new LiteralExpression(AkType.LONG, (long) pos);
        
        Expression top = new LocateExpression(Arrays.asList(subEx, strEx, posEx));
        if (expectNull) 
            assertTrue("expect locate (" + substr + ", " + str + ", " + pos + "), expect null, but was not null", top.evaluation().eval().isNull()); 
        else {
            long actual = top.evaluation().eval().getLong();
            assertTrue("expect locate (" + substr + ", " + str + ", " + pos + ") = " + expected + " but was " + actual,
                    actual == expected);
        }
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }
    
    @Override
    protected ExpressionComposer getComposer() 
    {
        return LocateExpression.COMPOSER;
    }
}
