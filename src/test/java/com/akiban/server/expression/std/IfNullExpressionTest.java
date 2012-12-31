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
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith (NamedParameterizedRunner.class)
public class IfNullExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private boolean expectExc;
    private int nargs;

    public IfNullExpressionTest (int nargs, boolean expectExc)
    {
        this.nargs = nargs;
        this.expectExc = expectExc;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params ()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
        
        param(pb, 0, true);
        param(pb, 1, true);
        param(pb, 2, false);

        for (int n = 3; n < 20; ++n)
            param(pb, n, true);

        return pb.asList();
    }

    private static void param (ParameterizationBuilder pb, int nargs, boolean error)
    {
        pb.add("Children count = " + nargs + "---Expect WrongArityException? " + error, nargs, error);
    }

    @OnlyIf("expectExc()")
    @Test (expected=WrongExpressionArityException.class)
    public void testWithExc()
    {
        testArity(nargs);
    }

    @OnlyIfNot("expectExc()")
    @Test
    public void testWithoutExc()
    {
        testArity(nargs);
    }

    private static void testArity (int nargs)
    {
        alreadyExc = true;
        Expression arg = new LiteralExpression(AkType.LONG, 1);
        List<Expression> args = new ArrayList<Expression>(nargs);
        for (int n = 0; n < nargs; ++n)
            args.add(arg);
        new IfNullExpression(args);
        
    }

    public boolean expectExc ()
    {
        return expectExc;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.LONG, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return IfNullExpression.IFNULL_COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
