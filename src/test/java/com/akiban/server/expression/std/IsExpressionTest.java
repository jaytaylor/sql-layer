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
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static com.akiban.server.expression.std.IsExpression.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class IsExpressionTest extends ComposedExpressionTestBase
{
    private Expression arg;
    private boolean expected;
    ExpressionComposer composer;

    private static boolean alreadyExc = false;
    private static final Boolean [] ARGS = new Boolean [] {true, false, null};

    public IsExpressionTest (Expression arg, boolean expected, ExpressionComposer composer)
    {
        this.arg = arg;
        this.expected = expected;
        this.composer = composer;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params ()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // is true
        param(pb, new boolean [] {true, false, false}, IS_TRUE);

        // is false
        param(pb, new boolean [] {false, true, false}, IS_FALSE);

        // is unknown
        param(pb, new boolean [] {false, false, true}, IS_UNKNOWN);

        return pb.asList();
    }

    private static void param (ParameterizationBuilder pb, boolean expected[], ExpressionComposer composer)
    {
        for (int n = 0; n < 3; ++n)
            pb.add (ARGS[n] + " " + composer + " --->" + expected[n],
                    LiteralExpression.forBool(ARGS[n]), expected[n], composer);
    }

    @Test
    public void test ()
    {
        Expression top = composer.compose(Arrays.asList(arg));

        assertTrue("Top is BOOL", top.valueType() == AkType.BOOL);
        assertEquals(expected, top.evaluation().eval().getBool());
        alreadyExc = true;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.BOOL, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return composer;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }    
}
