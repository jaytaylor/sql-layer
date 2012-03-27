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
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.OnlyIf;
import com.akiban.server.error.WrongExpressionArityException;
import org.junit.Test;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.LogExpression.*;

@RunWith(NamedParameterizedRunner.class)
public class LogExpressionTest extends ComposedExpressionTestBase
{
    private ExpressionComposer composer;
    private Double [] args;
    private Double expected;
    private boolean expectException;

    private static boolean alreadyExc = false;

    public LogExpressionTest (Double expected, ExpressionComposer composer, boolean expectException, Double ... args)
    {
        this.expected = expected;
        this.composer = composer;
        this.args = args;
        this.expectException = expectException;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // log
        param(pb, Math.log(1), LOG, false, 1.0); // log(10 => 0.0
        param(pb, null, LOG, false, 0.0); // log (0) => undefined
        param(pb, null, LOG, false, -2.0); // log  (negative num) => undefined
        param(pb, null, LOG, false, 1.0, 2.0); // log (1, 2) ==> log (2) / log(1) => undefined
        param(pb, null, LOG, false, -2.3, 2.5);
        param(pb, null, LOG, false, 2.3, -2.5);
        param(pb, null, LOG, false, -2.3, -2.5);
        param(pb, Math.log(2.5) / Math.log(2.3), LOG, false, 2.3, 2.5);
        param(pb, null, LOG, true, 2.0, 3.0, 4.0); // wrong arity exception

        // test specifal values
        for (double val : Arrays.asList(Double.POSITIVE_INFINITY, Double.NaN))
        {
            param(pb, val, LOG, false, val); // LN(infinity) = infinity
            param(pb, null, LOG, false, val, 2.0); // LOG_base_infinity is undefined
            param(pb, null, LOG, false, val, val); // ditto
        }

        param(pb, null, LOG, false, Double.NEGATIVE_INFINITY);
        param(pb, null, LOG, false, Double.NEGATIVE_INFINITY, 2.0);
        param(pb, null, LOG, false, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);

        // log10, log2, ln
        for (ExpressionComposer cp : Arrays.asList(LOG10, LOG2, LN))
        {
            param(pb, null, cp, true, 3.0, 4.0); // log10, log2, ln only take 1 arg
            param(pb, null, cp, true, 3.0, 4.0, 5.0); // ditto
            param(pb, null, cp, true); // no- arg , expect wrong arity exception
            param(pb, null, cp, false, 0.0); // log_x (0) => undefined
            param(pb, null, cp, false, -2.0); // log_x  (negative num) => undefined
            param(pb, 0.0, cp, false, 1.0); // log_x(1) = 0 regardless of base
            param(pb, null, cp, false, (Double)null); // input is null
            param(pb, Double.POSITIVE_INFINITY, cp, false, Double.POSITIVE_INFINITY);
            param(pb, null, cp, false, Double.NEGATIVE_INFINITY);
            param(pb, Double.NaN, cp, false, Double.NaN);
        }

        // log10
        param(pb, Math.log10(2.5) , LOG10, false, 2.5);

        // log2
        param(pb, Math.log(5.123) / Math.log(2), LOG2, false, 5.123);

        // ln
        param(pb, Math.log(6.67), LN, false, 6.67);

        return pb.asList();

    }

    private static void param (ParameterizationBuilder pb,
            Double expected, ExpressionComposer cp, boolean expectException, Double...args)
    {
        pb.add(cp.toString() + "(" + generateName(args) + ") ", expected, cp, expectException, args);
    }

    private static String generateName (Double...input)
    {
        if (input.length == 0) return "NULL";
        StringBuffer buffer = new StringBuffer();

        int n = 0;
        for (; n < input.length -1 ; ++n)
            buffer.append(input[n] + ", ");
        buffer.append(input[n]);

        return buffer.toString();
    }

    @OnlyIf("expectException()")
    @Test(expected = WrongExpressionArityException.class)
    public void testWithExc ()
    {
        test();
    }

    @OnlyIfNot("expectException()")
    @Test
    public void testWithoutExc ()
    {
        test();
    }

    private void test ()
    {
        ValueSource top = composer.compose(getArgsList()).evaluation().eval();
        if (expected == null)
            assertTrue("Top should be null", top.isNull());
        else
            assertEquals(expected, top.getDouble(), 0.0001);
        alreadyExc = true;
    }

    public boolean expectException ()
    {
        return expectException;
    }

    private  List<Expression> getArgsList ()
    {
        List<Expression> rst = new ArrayList(args.length);
        for (Double arg : args)
            rst.add( arg == null ?
                    LiteralExpression.forNull() :
                    new LiteralExpression(AkType.DOUBLE, arg.doubleValue()));
        return rst;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return LOG;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
