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

import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import com.akiban.server.expression.ExpressionComposer;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class HexExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private Expression arg;
    private String expected;
    
    public HexExpressionTest(Expression op, String exp)
    {
        arg = op;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> param()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
       
        param(pb, lit(null), null);
        param(pb, lit(32), "20");
        param(pb, lit(65), "41");
                
        param(pb, lit("\n"), "0A");
        param(pb, lit("abc"), "616263");
        //param(pb, lit("☃"), "E29883"); UTF8 charset.
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder bp, Expression arg, String exp)
    {
        bp.add("HEX(" + arg + ") ", arg, exp);
    }
    
    @Test
    public void test()
    {
        Expression top = new HexExpression(arg);
        
        if (expected == null)
            assertTrue ("Top should be null ", top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
        alreadyExc = true;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return HexExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
