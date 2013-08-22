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

import com.foundationdb.server.error.WrongExpressionArityException;
import java.util.List;
import com.foundationdb.server.types.ValueSourceIsNullException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class LengthExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.LONG, true);

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
        compose(getComposer(), getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        compose(getComposer(), getArgList());
        
        // null arguments
        compose(getComposer(), getArgList(ExprUtil.constNull(AkType.VARCHAR)));
    }
    
   
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
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

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
