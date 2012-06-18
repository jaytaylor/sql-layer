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
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class LeftRightExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String st;
    private Integer len;
    private String expected;
    private Integer argc;
    private final ExpressionComposer composer;
    
    public LeftRightExpressionTest(String str, Integer length, String exp, Integer count, ExpressionComposer com)
    {
        st = str;
        len = length;
        expected = exp;
        argc = count;
        composer = com;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        String name;
        testLeft(pb, name = "Test Shorter Length", "abc", 2, "ab", null);
        testLeft(pb, name, "abc", 0, "", null);
        testLeft(pb, name, "abc", -4, "", null);
        
        testLeft(pb, name = "Test Longer Length", "abc", 4, "abc", null);
        testLeft(pb, name, "abc", 3, "abc", null);
        
        testLeft(pb, name = "Test NULL", null, 3, null, null);
        testLeft(pb, name, "ab", null, null, null);
        
        testLeft(pb, name = "Test Wrong Arity", null, null, null, 0);
        testLeft(pb, name, null, null, null, 1);
        testLeft(pb, name, null, null, null, 3);
        testLeft(pb, name, null, null, null, 4);
        testLeft(pb, name, null, null, null, 5);
        
        return pb.asList();
    }
    
    private static void testLeft (ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " LEFT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc, LeftRightExpression.LEFT_COMPOSER);
    }
    
    private static void testRight(ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " RIGHT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc, LeftRightExpression.RIGHT_COMPOSER);
    }
    
    @OnlyIfNot("testArity()")
    @Test
    public void testRegularCases()
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = len == null? LiteralExpression.forNull():
                            new LiteralExpression(AkType.LONG,  len.intValue());
        
        Expression top = composer.compose(Arrays.asList(str, length));
        
        assertEquals("LEFT(" + st + ", " + len + ") ", 
                    expected == null? NullValueSource.only() : new ValueHolder(AkType.VARCHAR, expected),
                    top.evaluation().eval());
        alreadyExc = true;
    }
    
    @OnlyIf("testArity()")
    @Test(expected = WrongExpressionArityException.class)
    public void testFunctionArity()
    {
        List<Expression> args = new ArrayList<Expression>();
        for (int n = 0; n < argc; ++n)
            args.add(LiteralExpression.forNull());
        composer.compose(args);
        alreadyExc = true;
    }
    
    public boolean testArity()
    {
        return argc != null;
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
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
