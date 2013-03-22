

package com.akiban.server.expression.std;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.util.Collection;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(NamedParameterizedRunner.class)
public class PowExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private Double base;
    private Double exponent;
    private Double expected;
    
    public PowExpressionTest (Double a, Double n, Double exp)
    {
        base = a;
        exponent = n;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        // test regular cases
        param(pb, 2d, 2d, 4d);
        param(pb, 0.5d, 2d, 0.25d);
        
        // test NULLs
        param(pb, null, 2d, null);
        param(pb, 4d, null, null);
        param(pb, null, null, null);
        
        // test infinity
        param(pb, Double.POSITIVE_INFINITY, 2d, Double.POSITIVE_INFINITY);
        param(pb, Double.NEGATIVE_INFINITY, 3d, Double.NEGATIVE_INFINITY);
        param(pb, Double.NEGATIVE_INFINITY, 4d, Double.POSITIVE_INFINITY);
        param(pb, 1d, (double)Double.MAX_EXPONENT, 1d);
        
        // test NaN
        param(pb, Double.NaN, 5d, Double.NaN);
        param(pb, 1d, Double.NaN, Double.NaN);
        param(pb, Double.NaN, Double.NaN, Double.NaN);
        
        // test negative exponent
        param(pb, -5d, -1d, -0.2);
        param(pb, 25d, -0.5d, 0.2);
        
        // test exponents in (0, 1)
        param(pb, 144d, 0.5, 12d);
        param(pb, 32.0, 0.2, 2d);
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, 
                              Double base, Double exponent, Double expected)
    {
        pb.add("POW(" + base + ", " + exponent + ") ", base, exponent, expected);
    }
    
    
    @Test
    public void test()
    {
        Expression left = base == null
                            ? LiteralExpression.forNull()
                            : new LiteralExpression(AkType.DOUBLE, base.doubleValue());
        
        Expression right = exponent == null
                            ? LiteralExpression.forNull()
                            : new LiteralExpression(AkType.DOUBLE, exponent.doubleValue());
        
        ValueSource top = new PowExpression(left, right).evaluation().eval();
        if (expected == null)
            assertTrue("Top should be NULL ", top.isNull());
        else
            assertEquals(expected.doubleValue(), top.getDouble(), 0.00001);
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return PowExpression.POW;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
