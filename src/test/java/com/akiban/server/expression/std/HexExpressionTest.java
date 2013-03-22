
package com.akiban.server.expression.std;

import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import com.akiban.server.expression.ExpressionComposer;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class HexExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private Expression arg;
    private String expected;
    
    public HexExpressionTest(Expression op, String exp)
    {
        arg = op;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> param()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
       
        param(pb, lit(null), null);
        param(pb, lit(32), "20");
        param(pb, lit(65), "41");
                
        param(pb, lit("\n"), "0A");
        param(pb, lit("abc"), "616263");
        //param(pb, lit("☃"), "E29883"); UTF8 charset.
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder bp, Expression arg, String exp)
    {
        bp.add("HEX(" + arg + ") ", arg, exp);
    }
    
    @Test
    public void test()
    {
        Expression top = new HexExpression(arg);
        
        if (expected == null)
            assertTrue ("Top should be null ", top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
        alreadyExc = true;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return HexExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
