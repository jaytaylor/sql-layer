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

import com.foundationdb.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class CRC32ExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
 
    private final String arg;
    private final Long expected;
    
    public CRC32ExpressionTest(String arg, Long expected)
    {
        this.arg = arg;
        this.expected = expected;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder b = new ParameterizationBuilder();
        
        test(b,
             "akiban",
             1192856190L);
        
        test(b,
             "",
             0L);
        
        test(b,
             null,
             null);

        return b.asList();
    }
    
    private static void test(ParameterizationBuilder b, String arg, Long exp)
    {
        b.add("CRC32(" + arg + ") ", arg, exp);
    }

    @Test
    public void test()
    {
        alreadyExc = true;
        
        Expression input = arg == null
                            ? LiteralExpression.forNull()
                            : new LiteralExpression(AkType.VARCHAR, arg);
        
        Expression top = new CRC32Expression(input, "latin1"); // mysql's most 'popular' charset
        
        ValueSource actual = top.evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be null ",actual.isNull());
        else
            assertEquals(expected.longValue(), actual.getLong());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return CRC32Expression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
