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
        getComposer().compose(getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        getComposer().compose(getArgList());
        
        // null arguments
        getComposer().compose(getArgList(ExprUtil.constNull(AkType.VARCHAR)));
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
