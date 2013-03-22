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

package com.akiban.server.expression.std;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.util.WrappingByteSource;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(NamedParameterizedRunner.class)
public class UnhexExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String input;
    private String output;
    
    public UnhexExpressionTest (String in, String out)
    {
        input = in;
        output = out;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
        
        pr(p, "4D7953514C", "MySQL");
        pr(p, "20", " ");
        
        pr(p, null, null);
        pr(p, "1234AG", null);
        
        return p.asList();
    }
    
    private static void pr(ParameterizationBuilder p, String in, String out)
    {
        p.add("UNHEX(" + in + ")", in, out);
    }
    
    @Test
    public void test()
    {
        alreadyExc = true;
        
        Expression top = new UnhexExpression(input == null
                ? LiteralExpression.forNull()
                : new LiteralExpression(AkType.VARCHAR, input));
        
        if (output == null)
            assertTrue("Top should be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(new WrappingByteSource(output.getBytes()), 
                         top.evaluation().eval().getVarBinary());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }
           

    @Override
    protected ExpressionComposer getComposer()
    {
        return UnhexExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
