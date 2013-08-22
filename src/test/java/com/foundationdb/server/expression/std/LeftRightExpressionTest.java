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

import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.junit.Parameterization;
import java.util.Collection;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class LeftRightExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String st;
    private Integer len;
    private String expected;
    private Integer argc;
    private final ExpressionComposer composer;
    
    public LeftRightExpressionTest(String str, Integer length, String exp, Integer count, ExpressionComposer com)
    {
        st = str;
        len = length;
        expected = exp;
        argc = count;
        composer = com;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        String name;
        testLeft(pb, name = "Test Shorter Length", "abc", 2, "ab", null);
        testLeft(pb, name, "abc", 0, "", null);
        testLeft(pb, name, "abc", -4, "", null);
        
        testLeft(pb, name = "Test Longer Length", "abc", 4, "abc", null);
        testLeft(pb, name, "abc", 3, "abc", null);
        
        testLeft(pb, name = "Test NULL", null, 3, null, null);
        testLeft(pb, name, "ab", null, null, null);
        
        testLeft(pb, name = "Test Wrong Arity", null, null, null, 0);
        testLeft(pb, name, null, null, null, 1);
        testLeft(pb, name, null, null, null, 3);
        testLeft(pb, name, null, null, null, 4);
        testLeft(pb, name, null, null, null, 5);
        
        return pb.asList();
    }
    
    private static void testLeft (ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " LEFT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc, LeftRightExpression.LEFT_COMPOSER);
    }
    
    private static void testRight(ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " RIGHT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc, LeftRightExpression.RIGHT_COMPOSER);
    }
    
    @OnlyIfNot("testArity()")
    @Test
    public void testRegularCases()
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = len == null? LiteralExpression.forNull():
                            new LiteralExpression(AkType.LONG,  len.intValue());
        
        Expression top = compose(composer, Arrays.asList(str, length));
        
        assertEquals("LEFT(" + st + ", " + len + ") ", 
                    expected == null? NullValueSource.only() : new ValueHolder(AkType.VARCHAR, expected),
                    top.evaluation().eval());
        alreadyExc = true;
    }
    
    @OnlyIf("testArity()")
    @Test(expected = WrongExpressionArityException.class)
    public void testFunctionArity()
    {
        List<Expression> args = new ArrayList<>();
        for (int n = 0; n < argc; ++n)
            args.add(LiteralExpression.forNull());
        compose(composer, args);
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
        return composer;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
