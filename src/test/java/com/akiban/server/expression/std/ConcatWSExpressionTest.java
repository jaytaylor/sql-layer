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

import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import java.util.List;
import java.util.ArrayList;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ConcatWSExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private final String expected;
    private final String args[];

    public ConcatWSExpressionTest(String expected, String ... args)
    {
        this.expected = expected;
        this.args = args;
    }

    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder bd = new ParameterizationBuilder();
        
        test(bd, "1a2a3", "a", "1", "2", "3");
        test(bd, null, null, "1", "2", "3");
        test(bd, "abcdef", "", "ab", "cd", "ef");
        test(bd, "", "", "", "");
        test(bd, "a,b,c,d", ",", "a", "b", null, "c", "d", null);
        return bd.asList();
    }
    
    private static void test(ParameterizationBuilder b, String expected, String ... args)
    {
        b.add("CONCAT_WS(" + getName(args) + ") ", expected, args);
    }
    
    private static String getName(String ... args)
    {
        if (args == null)
            return "null";
        
        StringBuilder ret = new StringBuilder();
        for (String st : args)
            ret.append(st);
        
        return ret.toString();
    }

    @Test
    public void test()
    {
        alreadyExc = true;

        List<Expression> inputs = new ArrayList<Expression>();

        if (args != null)
            for (String st : args)
                inputs.add(st == null ? LiteralExpression.forNull()
                                      : new LiteralExpression(AkType.VARCHAR, st));
        
        Expression top = new ConcatWSExpression(inputs);
        ValueSource actual = top.evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be null ", actual.isNull());
        else
            assertEquals(expected, actual.getString());
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ConcatWSExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
