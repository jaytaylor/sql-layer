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
import java.util.Arrays;
import java.util.List;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.OnlyIf;
import com.akiban.server.types.ValueSourceIsNullException;
import org.junit.runner.RunWith;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class CaseConvertExpressionTest extends ComposedExpressionTestBase
{
    private String input;
    private String expected;
    private CaseConvertExpression.ConversionType convertType;

    private static boolean alreadyExecuted = false;
    
    public CaseConvertExpressionTest (String input, String expected, 
            CaseConvertExpression.ConversionType convertType)
    {
        this.input = input;
        this.expected = expected;
        this.convertType = convertType;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
       
        //------------TO LOWER TESTS--------------------------------------------
        // lowercase -> upper case
        param(pb, "lowercase string", "LOWERCASE STRING",
                CaseConvertExpression.ConversionType.TOUPPER);
        
        // null string -> upper
       param(pb, "", "", CaseConvertExpression.ConversionType.TOUPPER);
        
        // upper -> upper
        param(pb, "UPPERCASE", "UPPERCASE",
                CaseConvertExpression.ConversionType.TOUPPER);
        
        // null -> null : expect ValueSourceIsNull exception
        param(pb, null, null, CaseConvertExpression.ConversionType.TOUPPER);
        
        
        // --------------TOUPPER TESTS------------------------------------------
        // uppper -> lower
        param(pb, "UPPERCASE", "uppercase",
                CaseConvertExpression.ConversionType.TOLOWER);
        
        // null -> lower
        param(pb, "", "", CaseConvertExpression.ConversionType.TOLOWER);
        
        // lower -> lower
        param(pb, "lowercase", "lowercase",
                CaseConvertExpression.ConversionType.TOLOWER);  
      
        // null -> null : expect ValueSourceIsNull exception
        param(pb, null, null, CaseConvertExpression.ConversionType.TOLOWER);
        
        return pb.asList();
    }
    
     private static void param(ParameterizationBuilder pb,String input, String expected,
             CaseConvertExpression.ConversionType convertType)
     {
         pb.add(convertType.name() + input, input, expected, convertType);
         
     }
     
      
     @OnlyIfNot ("isAlreadyExecuted()")
     @Test (expected=WrongExpressionArityException.class)
     public void illegalArgTest ()
     {
        // excessive arguments
        getComposer().compose(getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        getComposer().compose(getArgList());
        
        // null argument
        getComposer().compose(getArgList(ExprUtil.constNull(AkType.VARCHAR)));
    
        alreadyExecuted = true;
     }
    
     @OnlyIf("expectNullException()")
     @Test(expected=ValueSourceIsNullException.class)
     public void expectingExceptionTest() {
         test();
     }
          
     @OnlyIfNot("expectNullException()")
     @Test()
     public void notExpectingExceptionTest() {
         test();
     }
     
 
    @Override
    protected int childrenCount() 
    {
        return 1;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return (convertType == CaseConvertExpression.ConversionType.TOLOWER ?
                CaseConvertExpression.TOLOWER_COMPOSER:
                CaseConvertExpression.TOUPPER_COMPOSER);
    }
    
      public boolean expectNullException() 
    {
        return (input == null);
    }
    
    public boolean isAlreadyExecuted()
    {
        return alreadyExecuted;
    }
    
    private static List<? extends Expression> getArgList (Expression...st)
    {
        return Arrays.asList(st);
    }
        
    private void test ()
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression expression = new CaseConvertExpression(inputExp,
                convertType);
        ValueSource result = expression.evaluation().eval();
        String actual = result.getString();
        
        assertTrue("Actual equals expected", actual.equals(expected));
    }
    
}
