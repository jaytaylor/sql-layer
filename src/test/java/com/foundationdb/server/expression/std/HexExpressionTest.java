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

import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.expression.ExpressionComposer;
import java.util.Collection;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import org.junit.runner.RunWith;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.foundationdb.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class HexExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private Expression arg;
    private String expected;
    
    public HexExpressionTest(Expression op, String exp)
    {
        arg = op;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> param()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
       
        param(pb, lit(null), null);
        param(pb, lit(32), "20");
        param(pb, lit(65), "41");
                
        param(pb, lit("\n"), "0A");
        param(pb, lit("abc"), "616263");
        //param(pb, lit("☃"), "E29883"); UTF8 charset.
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder bp, Expression arg, String exp)
    {
        bp.add("HEX(" + arg + ") ", arg, exp);
    }
    
    @Test
    public void test()
    {
        Expression top = new HexExpression(arg);
        
        if (expected == null)
            assertTrue ("Top should be null ", top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
        alreadyExc = true;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return HexExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
