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

import org.junit.runner.RunWith;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.expression.Expression;
import java.util.List;
import java.util.ArrayList;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ConcatWSExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private final String expected;
    private final String args[];

    public ConcatWSExpressionTest(String expected, String ... args)
    {
        this.expected = expected;
        this.args = args;
    }

    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder bd = new ParameterizationBuilder();
        
        test(bd, "1a2a3", "a", "1", "2", "3");
        test(bd, null, null, "1", "2", "3");
        test(bd, "abcdef", "", "ab", "cd", "ef");
        test(bd, "", "", "", "");
        test(bd, "a,b,c,d", ",", "a", "b", null, "c", "d", null);
        return bd.asList();
    }
    
    private static void test(ParameterizationBuilder b, String expected, String ... args)
    {
        b.add("CONCAT_WS(" + getName(args) + ") ", expected, args);
    }
    
    private static String getName(String ... args)
    {
        if (args == null)
            return "null";
        
        StringBuilder ret = new StringBuilder();
        for (String st : args)
            ret.append(st);
        
        return ret.toString();
    }

    @Test
    public void test()
    {
        alreadyExc = true;

        List<Expression> inputs = new ArrayList<>();

        if (args != null)
            for (String st : args)
                inputs.add(st == null ? LiteralExpression.forNull()
                                      : new LiteralExpression(AkType.VARCHAR, st));
        
        Expression top = new ConcatWSExpression(inputs);
        ValueSource actual = top.evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be null ", actual.isNull());
        else
            assertEquals(expected, actual.getString());
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ConcatWSExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
