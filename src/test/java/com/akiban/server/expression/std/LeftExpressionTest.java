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
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class LeftExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String st;
    private Integer len;
    private String expected;
    private Integer argc;
    
    public LeftExpressionTest(String str, Integer length, String exp, Integer count)
    {
        st = str;
        len = length;
        expected = exp;
        argc = count;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        String name;
        param(pb, name = "Test Shorter Length", "abc", 2, "ab", null);
        param(pb, name, "abc", 0, "", null);
        param(pb, name, "abc", -4, "", null);
        
        param(pb, name = "Test Longer Length", "abc", 4, "abc", null);
        param(pb, name, "abc", 3, "abc", null);
        
        param(pb, name = "Test NULL", null, 3, null, null);
        param(pb, name, "ab", null, null, null);
        
        param(pb, name = "Test Wrong Arity", null, null, null, 0);
        param(pb, name, null, null, null, 1);
        param(pb, name, null, null, null, 3);
        param(pb, name, null, null, null, 4);
        param(pb, name, null, null, null, 5);
        
        return pb.asList();
    }
    
    private static void param (ParameterizationBuilder pb, String name, String str, Integer length, String exp, Integer argc)
    {
        pb.add(name + " LEFT(" + str + ", " + length + "), argc = " + argc, str, length, exp, argc);
    }
    
    
    @OnlyIfNot("testArity()")
    @Test
    public void testRegularCases()
    {
        Expression str = new LiteralExpression(AkType.VARCHAR, st);
        Expression length = len == null? LiteralExpression.forNull():
                            new LiteralExpression(AkType.LONG,  len.intValue());
        
        Expression top = new LeftExpression(str, length);
        
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
        compose(LeftExpression.COMPOSER, args);
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
        return LeftExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
