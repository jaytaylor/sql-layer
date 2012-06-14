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

import java.util.Arrays;
import com.akiban.server.error.WrongExpressionArityException;
import org.junit.Test;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

import static org.junit.Assert.*;

public class RadExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        doTest(180.0, Math.PI);
        doTest(90.0, Math.PI/2);
        doTest(45.0, Math.PI/4);
        doTest(30.0, Math.PI/6);
        doTest(60.0, Math.PI/3);
        doTest(270.0, 3* Math.PI / 2);
        doTest(null, null);
    }
    
    @Test(expected=WrongExpressionArityException.class)
    public void testArity()
    {
        Expression top = RandExpression.COMPOSER.compose(Arrays.asList(LiteralExpression.forNull(), LiteralExpression.forNull()));
    }
    
    private static void doTest(Double input, Double expected)
    {
        Expression top = new RadExpression(input == null ? LiteralExpression.forNull()
                                                         :new LiteralExpression(AkType.DOUBLE, input.doubleValue()));
        String testName = "[RAD(" + input + ")]";
        
        if (expected == null)
            assertTrue(testName + " Top shoud be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(testName, expected.doubleValue(), top.evaluation().eval().getDouble(), 0.0001);
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return RadExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
