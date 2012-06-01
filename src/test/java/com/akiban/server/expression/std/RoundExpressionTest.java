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

import java.math.BigInteger;
import java.math.BigDecimal;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;

import static com.akiban.server.expression.std.ExprUtil.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class RoundExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;

    private List<? extends Expression> args;
    private Expression expected;
    
    public RoundExpressionTest(List<? extends Expression> args, Expression exp)
    {
        this.args = args;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
        
        // test with primitive types (double/float, long/int)
        param(p, Arrays.asList(lit(150.12345), lit(2)), lit(150.12));
        param(p, Arrays.asList(lit(123.3f), lit(-1)), lit(120.0f));
        param(p, Arrays.asList(lit(12L), lit(-1L)), lit(10L));
        param(p, Arrays.asList(lit(123.4), lit(-10L)), lit(0.0d));
        param(p, Arrays.asList(lit(16.1234), lit(-1)), lit(20.0));
        param(p, Arrays.asList(lit(5L), lit(3L)), lit(5L));
        param(p, Arrays.asList(lit(10L), lit(-5L)), lit(0L));
        param(p, Arrays.asList(lit(0.49999d), lit(0L)), lit(0.0));
        
        // test with objects (BigDecimal/String/BigInteger)
        param(p, 
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, BigDecimal.valueOf(5.123000).setScale(6)),
                              lit(4L)), 
                new LiteralExpression(AkType.DECIMAL, BigDecimal.valueOf(5.1230).setScale(4)));
        
        param(p,
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, BigDecimal.valueOf(12345.123).setScale(3)),
                              lit(-2L)),
                new LiteralExpression(AkType.DECIMAL, BigDecimal.valueOf(12300).setScale(0)));
        
        param(p,
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, BigDecimal.valueOf(123.0).setScale(1)),
                              lit(-10L)),
                new LiteralExpression(AkType.DECIMAL, BigDecimal.ZERO));
        
        param(p,
              Arrays.asList(new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(16)),
                            lit(-1L)),
              new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(20)));
        
        param(p,
              Arrays.asList(new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(12)),
                            lit(4L)),
              new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(12)));
        
        param(p, Arrays.asList(lit("123.456")), lit(123.0d));
        param(p, Arrays.asList(lit("456.780"), lit(-2L)), lit(500.0d));
        param(p, Arrays.asList(lit("12"), lit(-5L)), lit(0.0d));
        
        // test with nulls
        param(p, Arrays.asList(LiteralExpression.forNull()), null);
        param(p, Arrays.asList(lit(12.3d), LiteralExpression.forNull()), null);
        param(p, Arrays.asList(LiteralExpression.forNull(), LiteralExpression.forNull()), null);
        
        // test invalid arg type
        param(p, Arrays.asList(new LiteralExpression(AkType.DATE, 231231L)), null);
        param(p, Arrays.asList(new LiteralExpression(AkType.TIME, 123010L), lit(2L)), null);
        
        return p.asList();
    }
    
    private static void param(ParameterizationBuilder p, List<? extends Expression> args, Expression exp)
    {
        p.add("ROUND(" + args + ")" , args, exp);
    }
    
    @Test
    public void test()
    {
        alreadyExc = true;
       
        ValueSource top = new RoundExpression(args).evaluation().eval();        
        if (expected == null)
            assertTrue("Top should be NULL", top.isNull());
        else
            assertEquals(new ValueHolder(expected.evaluation().eval()), 
                         new ValueHolder(top));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.LONG, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return RoundExpression.ROUND;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
