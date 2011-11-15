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
import com.akiban.server.types.ValueSourceIsNullException;
import java.util.List;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class SubStringExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(3, AkType.VARCHAR, true);

    @Test (expected=WrongExpressionArityException.class)
    public void testIllegalArg() 
    {
        // excessive arguments
        getComposer().compose(getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
        
        // insufficent arguments
        getComposer().compose(getArgList());
        
        // null arguments
        getComposer().compose(getArgList(ExprUtil.constNull(AkType.VARCHAR), 
                ExprUtil.constNull(AkType.VARCHAR), ExprUtil.constNull(AkType.VARCHAR)));
    }
    
    @Test(expected = ValueSourceIsNullException.class)
    public void test ()
    {
        // test with 2 argument  
        subAndCheck("1234", "1234", 1); // meaning from index 0 to the end
        subAndCheck("1234", "4", 0); // meaning from index 3
        subAndCheck("1234", "", 9);  
        subAndCheck("1234", "234", -3); // meanding from index 1
        subAndCheck ("", "", 5); // empty string -> empty string
        subAndCheck(null,null,4);
        
        // test with 3 argument
        subAndCheck ("1234", "4", 0, 2); // meaning from index 3 till 4
        subAndCheck ("1234", "4",  -1, 4); // meanding from index 3 til 4
        subAndCheck ("1234", "1234", -4, 4); // meaning from index 0 till inde 3
        subAndCheck ("", "", -5, 9); // empty string -> empty string
        subAndCheck ("1234", "", 1, -5); // length < 0 => empty string
    }
   

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return SubStringExpression.COMPOSER;
    }
    
    // ---------------------private methods ------------------------------------
    
    private static void subAndCheck (String st,String expected, int from )
    {
        check(substr(st,from), expected);
    }
    
    private static void subAndCheck (String st, String expected, int from, int length)
    {
        check(substr(st,from,length), expected);
    }
    
    private static void check (Expression inputExp, String expected)
    {
        ValueSource result = inputExp.evaluation().eval();
        String actual = result.getString();
    
        assertTrue ("Actual equals expected ", actual.equals(expected));
    }
    
    private static List <? extends Expression> getArgList (Expression ...arg)
    {     
        return Arrays.asList(arg);
    }
    
    private static Expression substr(String st, int from)
    {
        return new SubStringExpression(getArgList(ExprUtil.lit(st), ExprUtil.lit(from)));
    }
    
    private static Expression substr(String st, int from, int length)
    {
        return new SubStringExpression(getArgList(ExprUtil.lit(st), 
                ExprUtil.lit(from), ExprUtil.lit(length)));
    }
}
