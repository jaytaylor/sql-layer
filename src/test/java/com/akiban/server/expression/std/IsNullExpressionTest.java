

package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;

import com.akiban.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class IsNullExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.BOOL, false);

    @Test
    public void testNullExpression ()
    {
        Expression  expression = new IsNullExpression(new LiteralExpression(AkType.NULL, null));
        ValueSource source = expression.evaluation().eval();
        assertTrue (source.getBool());
    }
    
    @Test 
    public void testNotnull ()
    {
        Expression expression = new IsNullExpression (new LiteralExpression(AkType.DOUBLE, 1.0));
        ValueSource source = expression.evaluation().eval();
        assertFalse(source.getBool());
    }   
 
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return IsNullExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
