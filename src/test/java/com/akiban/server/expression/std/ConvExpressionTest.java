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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ConvExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;

    private String num;
    private Long from;
    private Long to;
    private String expected;
    
    public ConvExpressionTest (String num, Long from, Long to, String expected)
    {
        this.num = num;
        this.from = from;
        this.to = to;
        this.expected = expected;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
        
        String dec = "255";
        String hex = "FF";
        String bin = "11111111";
        
        param(p, hex, 16L, 10L, dec);
        param(p, hex, 16L, 2L, bin);
        param(p, bin, 2L, 10L, dec);
        param(p, bin, 2L, 16L, hex);
        param(p, dec, 10L, 16L, hex);
        param(p, dec, 10L, 2L, bin);
        
        // test convert to same base
        String val;
        param(p, val = "A", 16L, 16L, val);
        param(p, val = "123231231", 15L, 15L, val);
        param(p, "1234", 2L, 2L, "0");
        
        // test error
        param(p, hex, 10L, 2L, "0");
        param(p, "123", 56L, 3L, null);
        param(p, "10", 2L, 1L, null);
        param(p, "0", 1L, 0L, null);
        
        // test signed
        param(p, "-1", 10L, 16L, "FFFFFFFFFFFFFFFF");
        return p.asList();
    }
    
    private static void param(ParameterizationBuilder p, String num, Long from, Long to, String exp)
    {
        p.add("CONV(" + num + ", " + from + ", " + to + ") ", num, from, to, exp);
    }
    
    private static Expression getExp(Object val)
    {
        return val == null 
                ? LiteralExpression.forNull()
                : val instanceof Long
                    ? new LiteralExpression(AkType.LONG, ((Long)val).longValue())
                    : new LiteralExpression (AkType.VARCHAR, (String)val);
    }
    
    @Test
    public void test()
    {
        alreadyExc = true;
        Expression top = new ConvExpression(Arrays.asList(getExp(num),
                                                          getExp(from),
                                                          getExp(to)));
        
        if (expected == null)
            assertTrue("Top should be NULL ", top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ConvExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
