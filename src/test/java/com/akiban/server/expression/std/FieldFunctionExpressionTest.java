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

import com.akiban.junit.OnlyIf;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.Test;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;
import static com.akiban.server.types.AkType.*;

@RunWith(NamedParameterizedRunner.class)
public class FieldFunctionExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private List<? extends Expression> args;
    private Long expected;
    
    public FieldFunctionExpressionTest (List<? extends Expression> args, Long expected)
    {
        this.args = args;
        this.expected = expected;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        // easy cases: all args have the same type
        param(pb, 0L, lit(2L), lit(1L));
        param(pb, 4L, lit(3.6), lit(3.0), lit(3.59), lit(3.7), lit(3.6));
        param(pb, 4L, lit("00.1"), lit("666"), lit("0.1"), lit("777"), lit("00.1"));
        param(pb, 0L, lit(true), lit(false));
        param(pb, 2L, new LiteralExpression(DATETIME, 20091107123010L),
                      new LiteralExpression(DATETIME, 20091107123009L),
                      new LiteralExpression(DATETIME, 20091107123010L),
                      new LiteralExpression(DATETIME, 20091107123011L));
        
        param(pb, 0L, new LiteralExpression(TIME, 123010L),
                      new LiteralExpression(TIME, 123011L));
        
        // heterogeneous types
        param(pb, 3L, lit(123), LiteralExpression.forNull(), lit("123.1"), lit(123));
        param(pb, 2L, lit("00.1"), lit("666"), lit("0.1"), lit("777"), lit(true));
        param(pb, 0L, lit("00.1"), lit("666"), lit("0.11"), lit("777"), lit(true));
        param(pb, 2L, lit("12:30:10"), lit(123010L), new LiteralExpression(TIME, 123010L));
        param(pb, 3L, new LiteralExpression(TIME, 123010L), 
                      lit(12345L),
                      lit("123010.1"),
                      lit("123010"));
        param(pb, 2L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit("00:00:01"));
        param(pb, 0L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit("00:00:02"));
        param(pb, 2L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit(1000L));
        
        param(pb, 1L, new LiteralExpression(DATETIME, 19910510073010L),
                      lit ("19910510073010"));
        
        param(pb, 1L, new LiteralExpression(DATETIME, 19910510073010L),
                      lit("1991-05-10 07:30:10"));
        
        // this returns 0L
        // but probably should returns 1L
        param(pb, 0L, lit("1991-05-10 07:30:10"),
                      lit("19910510073010"),
                      lit(1L));
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, Long exp, Expression...args)
    {
        List<? extends Expression> argsList = Arrays.asList(args);
        pb.add("FIELD("+ argsList +") expcted: " + (exp == null ? "NULL" : exp),
                argsList,
                exp);
    }
    
    @OnlyIfNot("expectArityException()")
    @Test
    public void test()
    {
        doTest();
    }
    
    @OnlyIf("expectArityException()")
    @Test(expected=WrongExpressionArityException.class)
    public void testArity()
    {
        doTest();
    }
    
    private void doTest()
    {
        alreadyExc = true;
        Expression top = new FieldFunctionExpression(args);
        assertEquals(expected.longValue(), top.evaluation().eval().getLong());
    }
    
    public boolean expectArityException ()
    {
        return expected == null;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, VARCHAR, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return FieldFunctionExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
