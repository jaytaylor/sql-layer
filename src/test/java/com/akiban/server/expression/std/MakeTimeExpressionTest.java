/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

public class MakeTimeExpressionTest extends ComposedExpressionTestBase
{
    @Test(expected = WrongExpressionArityException.class)
    public void testIllegalArg()
    {
        List<Expression> argList = new ArrayList<Expression>();

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
        List<Expression> argList = new ArrayList<Expression>();
        
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
