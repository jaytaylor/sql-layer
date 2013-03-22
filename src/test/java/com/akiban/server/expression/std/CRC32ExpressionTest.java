
package com.akiban.server.expression.std;

import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class CRC32ExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
 
    private final String arg;
    private final Long expected;
    
    public CRC32ExpressionTest(String arg, Long expected)
    {
        this.arg = arg;
        this.expected = expected;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder b = new ParameterizationBuilder();
        
        test(b,
             "akiban",
             1192856190L);
        
        test(b,
             "",
             0L);
        
        test(b,
             null,
             null);

        return b.asList();
    }
    
    private static void test(ParameterizationBuilder b, String arg, Long exp)
    {
        b.add("CRC32(" + arg + ") ", arg, exp);
    }

    @Test
    public void test()
    {
        alreadyExc = true;
        
        Expression input = arg == null
                            ? LiteralExpression.forNull()
                            : new LiteralExpression(AkType.VARCHAR, arg);
        
        Expression top = new CRC32Expression(input, "latin1"); // mysql's most 'popular' charset
        
        ValueSource actual = top.evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be null ",actual.isNull());
        else
            assertEquals(expected.longValue(), actual.getLong());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return CRC32Expression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
