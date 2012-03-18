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

import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
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

@RunWith(NamedParameterizedRunner.class)
public class LeftExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String st;
    private Integer len;
    private String expected;
    private Integer argc;
    
    public LeftExpressionTest(String str, Integer length, String exp, Integer count)
    {
        st = str;
        len = length;
        expected = exp;
        argc = count;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        String name;
        param(pb, name = "Test Shorter Length", "abc", 2, "ab", null);
        param(pb, name, "abc", 0, "", null);
        param(pb, name, "abc", -4, "", null);
        
        param(pb, name = "Test Longer Length", "abc", 4, "abc", null);
        param(pb, name, "abc", 3, "abc", null);
        
        param(pb, name = "Test NULL", null, 3, null, null);
        param(pb, name, "ab", null, null, null);
        
        param(pb, name = "Test Wrong Arity", null, null, null, 0);
        param(pb, name, null, null, null, 1);
        param(pb, name, null, null, null, 3);
        param(pb, name, null, null, null, 4);
        param(pb, name, null, null, null, 5);
        
        return pb.asList();
    }
    
    private static void param (ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " LEFT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc);
    }
    
    
    @OnlyIfNot("testArity()")
    @Test
    public void testRegularCases()
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = len == null? LiteralExpression.forNull():
                            new LiteralExpression(AkType.LONG,  len.intValue());
        
        Expression top = new LeftExpression(str, length);
        
        assertEquals("LEFT(" + st + ", " + len + ") ", 
                    expected == null? NullValueSource.only() : new ValueHolder(AkType.VARCHAR, expected),
                    top.evaluation().eval());
        alreadyExc = true;
    }
    
    @OnlyIf("testArity()")
    @Test(expected = WrongExpressionArityException.class)
    public void testFunctionArity()
    {
        List<Expression> args = new ArrayList<Expression>();
        for (int n = 0; n < argc; ++n)
            args.add(LiteralExpression.forNull());
        LeftExpression.COMPOSER.compose(args);
        alreadyExc = true;
    }
    
    public boolean testArity()
    {
        return argc != null;
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
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
