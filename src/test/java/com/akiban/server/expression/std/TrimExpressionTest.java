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

import org.junit.runner.RunWith;
import com.akiban.server.types.AkType;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class TrimExpressionTest extends ComposedExpressionTestBase
{
    private String input;
    private String expected;
    private TrimExpression.TrimType trimType;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.VARCHAR, true);

    private static boolean alreadyExc = false;
    public TrimExpressionTest (String input, String expected, 
            TrimExpression.TrimType trimType)
    {
        this.input = input;
        this.expected = expected;
        this.trimType = trimType;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
        
        // trim leading
        param(pb, "     leading  ", "leading  ", TrimExpression.TrimType.LEADING);
        param(pb, "       ", "", TrimExpression.TrimType.LEADING);
        param(pb, "", "", TrimExpression.TrimType.LEADING);
        param(pb, "l     eading ", "l     eading ", TrimExpression.TrimType.LEADING);
        
        // trim trailing
        param(pb, " trailing  ", " trailing", TrimExpression.TrimType.TRAILING);
        param(pb, "   ", "", TrimExpression.TrimType.TRAILING);
        param(pb, "trailin    g", "trailin    g", TrimExpression.TrimType.TRAILING );
        
        
        // trim both
        param(pb, "  trim both   ", "trim both", null);
        param(pb, "", "", null);
        param(pb, "  leading", "leading", null);
        param(pb, "trailing  ", "trailing", null);
       
        
        return pb.asList();
    }
    
     private static void param(ParameterizationBuilder pb,String input, String expected,
            TrimExpression.TrimType trimType)
     {
         pb.add((trimType != null? trimType.name() : "TRIM") + input, input, expected, trimType);
         
     }
     
    
    @Test
    public void test ()
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression expression = new TrimExpression(inputExp,
                trimType);
        ValueSource result = expression.evaluation().eval();
        String actual = result.getString();
        
        assertTrue("Actual equals expected", actual.equals(expected));
        alreadyExc = true;
    }
    
    
    //private void check

    @Override
    protected CompositionTestInfo getTestInfo ()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return (trimType == null?  
                TrimExpression.TRIM_COMPOSER :
                    (trimType == TrimExpression.TrimType.TRAILING ? TrimExpression.RTRIM_COMPOSER : 
                        TrimExpression.LTRIM_COMPOSER));
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
    
}
