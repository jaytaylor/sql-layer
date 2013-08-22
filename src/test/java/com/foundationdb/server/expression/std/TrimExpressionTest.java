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
import com.foundationdb.server.types.AkType;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class TrimExpressionTest extends ComposedExpressionTestBase
{
    private String input;
    private String trimChar;
    private String expected;
    private TrimExpression.TrimType trimType;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.VARCHAR, true);

    private static boolean alreadyExc = false;
    public TrimExpressionTest (String input, String expected, 
            TrimExpression.TrimType trimType)
    {
        this.input = input;
        this.expected = expected;
        this.trimType = trimType;
        trimChar = " ";
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
        Expression trimCharExp = new LiteralExpression (AkType.VARCHAR, trimChar);
        Expression expression = new TrimExpression(inputExp, trimCharExp, trimType);
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
