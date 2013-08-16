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

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

public class MakeTimeExpressionTest extends ComposedExpressionTestBase
{
    @Test(expected = WrongExpressionArityException.class)
    public void testIllegalArg()
    {
        List<Expression> argList = new ArrayList<>();

        argList.add(new LiteralExpression(AkType.LONG, 12));
        argList.add(new LiteralExpression(AkType.LONG, 12));
        argList.add(new LiteralExpression(AkType.LONG, 12));
        argList.add(new LiteralExpression(AkType.LONG, 12));

        Expression top = compose(MakeTimeExpression.COMPOSER, argList);
        top.evaluation().eval();
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testNoArgs()
    {
        List<Expression> argList = new ArrayList<>();
        
        Expression top = compose(MakeTimeExpression.COMPOSER, argList);
        top.evaluation().eval();
    }

    @Test
    public void testNull()
    {
        Expression arg = LiteralExpression.forNull();
        Expression top = new MakeTimeExpression(arg,arg,arg);

        assertTrue(top.evaluation().eval().isNull());
    }
    
    @Test
    public void testRegular()            
    {
        Expression top = new MakeTimeExpression(getExp(30),getExp(30),getExp(10));
        ValueSource source = top.evaluation().eval();
        
        assertEquals(303010, source.getTime());
    }
    
    @Test
    public void testOnesDigitInput()
    {
        Expression top = new MakeTimeExpression(getExp(01),getExp(01),getExp(01));
        ValueSource source = top.evaluation().eval();
        
        assertEquals(10101, source.getTime());
    }
    
    @Test
    public void testInvalidMinute()
    {
        Expression top = new MakeTimeExpression(getExp(20),getExp(60),getExp(10));
        ValueSource source = top.evaluation().eval();
        
        assertTrue(source.isNull());
    }
    
    @Test
    public void testInvalidMinuteNeg()
    {
       Expression top = new MakeTimeExpression(getExp(20),getExp(-3),getExp(10));
       ValueSource source = top.evaluation().eval();
        
       assertTrue(source.isNull()); 
    }
    
    @Test
    public void testInvalidSecond()
    {
        Expression top = new MakeTimeExpression(getExp(20),getExp(59),getExp(60));
        ValueSource source = top.evaluation().eval();
        
        assertTrue(source.isNull()); 
    }
    
    @Test
    public void testInvalidSecondNeg()
    {
        Expression top = new MakeTimeExpression(getExp(20),getExp(59),getExp(-4));
        ValueSource source = top.evaluation().eval();
        
        assertTrue(source.isNull()); 
    }
    
    @Test
    public void testHourNeg()
    {
        Expression top = new MakeTimeExpression(getExp(-1),getExp(10),getExp(10));
        ValueSource source = top.evaluation().eval();
        
        assertEquals(-11010, source.getTime());
    }
    
    private static Expression getExp(int num)
    {
        return new LiteralExpression(AkType.LONG, num);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.LONG, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return MakeTimeExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
