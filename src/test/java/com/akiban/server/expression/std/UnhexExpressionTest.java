
package com.akiban.server.expression.std;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.util.WrappingByteSource;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(NamedParameterizedRunner.class)
public class UnhexExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private String input;
    private String output;
    
    public UnhexExpressionTest (String in, String out)
    {
        input = in;
        output = out;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
        
        pr(p, "4D7953514C", "MySQL");
        pr(p, "20", " ");
        
        pr(p, null, null);
        pr(p, "1234AG", null);
        
        return p.asList();
    }
    
    private static void pr(ParameterizationBuilder p, String in, String out)
    {
        p.add("UNHEX(" + in + ")", in, out);
    }
    
    @Test
    public void test()
    {
        alreadyExc = true;
        
        Expression top = new UnhexExpression(input == null
                ? LiteralExpression.forNull()
                : new LiteralExpression(AkType.VARCHAR, input));
        
        if (output == null)
            assertTrue("Top should be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(new WrappingByteSource(output.getBytes()), 
                         top.evaluation().eval().getVarBinary());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }
           

    @Override
    protected ExpressionComposer getComposer()
    {
        return UnhexExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
