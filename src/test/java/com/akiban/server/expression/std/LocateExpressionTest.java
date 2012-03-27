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
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class LocateExpressionTest extends ComposedExpressionTestBase 
{    
    @Test
    public void test() 
    {
        //test 2 args
        testLocate("bar", "foobarbar", 4);
        testLocate("xbar", "foobarbar", 0);
        testLocate("", "bar", 1);
        testLocate("", "", 1);
        testLocate("a", "", 0);
        testLocate( null, "abc", 0);
        testLocate( "bar", null, 0);
        
        // test 3 args
        testLocate("bar", "foobarbar", 5L, 7);
        testLocate("bar", "foobarbar", -5L, 0);
        testLocate("", "foobarbar", 1L,1);
        testLocate("", "", 1L,1);
        testLocate("", "", 3L, 0);
        testLocate(null, "abc", 3L, 0);
        testLocate("abc", null, 3L,0);
        testLocate("abc", "abcd", null, 0);
        
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testWithExc ()
    {
        Expression dummy = new LiteralExpression(AkType.VARCHAR, "abc");
        Expression top = new LocateExpression(Arrays.asList(dummy, dummy, dummy, dummy));
    }
    
    private static void testLocate(String substr, String str, int expected) 
    {
        boolean expectNull;
        Expression strEx = new LiteralExpression((expectNull = str == null) ? AkType.NULL : AkType.VARCHAR, str);
        Expression subEx = new LiteralExpression((expectNull |= substr == null) ? AkType.NULL : AkType.VARCHAR, substr);
        
        check(expectNull, expected, subEx, strEx);
    }
    
    private static void testLocate(String substr, String str, Long pos, long expected)
    {
        boolean expectNull;
        Expression strEx = new LiteralExpression((expectNull = str == null) ? AkType.NULL : AkType.VARCHAR, str);
        Expression subEx = new LiteralExpression((expectNull |= substr == null) ? AkType.NULL : AkType.VARCHAR, substr);
        Expression posEx = new LiteralExpression((expectNull |= pos == null) ? AkType.NULL : AkType.LONG,  pos == null ? 0L : (long)pos);
                
        check(expectNull, expected, subEx, strEx, posEx);
    }

    private static void check ( boolean expectNull, long expected ,Expression ... ex)
    {
        Expression top = new LocateExpression(Arrays.asList(ex));
        if (expectNull)
            assertTrue (ex.toString(), top.evaluation().eval().isNull());
        else        
            assertEquals(ex.toString(), expected, top.evaluation().eval().getLong());        
    }

    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }
    
    @Override
    protected ExpressionComposer getComposer() 
    {
        return LocateExpression.LOCATE_COMPOSER;
    }

    @Override
    protected boolean alreadyExc() 
    {
        return false;
    }
}
