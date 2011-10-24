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
import java.util.List;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class LengthExpressionTest extends ComposedExpressionTestBase
{
    public LengthExpressionTest ()
    {
        super();
    }

    @Test (expected=ValueSourceIsNullException.class)
    public  void test()
    {
        String st;
        
        testLength (st = "String 1", (long)st.length());
        testLength (st = "", (long)st.length());
        testLength (st = null, 0);
    }
    
    @Test (expected=WrongExpressionArityException.class)
    public void testIllegalArg() 
    {
        // excessive arguments
        getComposer().compose(getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        getComposer().compose(getArgList());
        
        // null arguments
        getComposer().compose(getArgList(ExprUtil.constNull(AkType.VARCHAR)));
    }
    
   
    @Override
    protected int childrenCount() 
    {
        return 1;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return LengthExpression.COMPOSER;
    }
    
    
    private static List<? extends Expression> getArgList (Expression...st)
    {
        return Arrays.asList(st);
    }
    
    private  void testLength (String input, long expected)
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression expression = new LengthExpression (inputExp); 
        ValueSource source = expression.evaluation().eval();
       
        long actual = source.getLong();    
        assertEquals(expected, actual);
    }

}
