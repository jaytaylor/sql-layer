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
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import org.junit.Test;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class EltExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;;
    
    private List<? extends Expression> input;
    private ValueHolder expected;
    
    public EltExpressionTest (List<? extends Expression> in, ValueHolder exp)
    {
        input = in;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        param(pb, new ValueHolder(AkType.VARCHAR, "3"), lit(1L), lit("3"));
        param(pb, new ValueHolder(AkType.DOUBLE, 3.5), lit(2L), lit(3.0), lit(3.5));
        param(pb, new ValueHolder(AkType.DOUBLE, 3.5), lit(2L), lit(3.0), lit(3.5), constNull());
        param(pb, new ValueHolder(AkType.DATETIME, 20120318123010L), lit(3L), 
                       new LiteralExpression(AkType.DATETIME, 1L),
                       new LiteralExpression(AkType.DATETIME, 2L), 
                       new LiteralExpression(AkType.DATETIME, 20120318123010L));
        
        param(pb, ValueHolder.holdingNull(), constNull(), lit(2L));
        param(pb, ValueHolder.holdingNull(), lit(2L), lit("abc"), constNull());
        param(pb, ValueHolder.holdingNull(), lit(2L), lit("abc"));
        param(pb, ValueHolder.holdingNull(), lit(0), lit("a"));
        param(pb, ValueHolder.holdingNull(), lit(-5), lit("ab"));
        
        param(pb, null, lit(2));
        param(pb, null, null);
        param(pb, null);
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, ValueHolder exp, Expression ... inputs)
    {
        pb.add("ELT(" + inputs + ")", inputs == null ? Collections.singletonList(constNull()):Arrays.asList(inputs), exp);
    }
    
    @OnlyIfNot("expectExc()")
    @Test
    public void testRegular()
    {   
        assertEquals("ELT(" + input + ") ", expected, new ValueHolder(new EltExpression(input).evaluation().eval()));
        alreadyExc = true;
    }
    
    @OnlyIf("expectExc()")
    @Test(expected=WrongExpressionArityException.class)
    public void testNegative()
    {
        compose(EltExpression.COMPOSER, input);
        alreadyExc = true;
    }
    
    public boolean expectExc()
    {
        return expected == null;
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.LONG, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return EltExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
