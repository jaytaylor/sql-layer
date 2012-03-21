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

import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import com.akiban.server.expression.ExpressionComposer;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

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
