
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

public class TestNull 
{
    @Test
    public void test_forNullEqualsNewLiteralNull()
    {
        Expression litNull = LiteralExpression.forNull();
        Expression nullExp = new LiteralExpression(AkType.NULL, null);
        assertSame(litNull.evaluation(), nullExp.evaluation());
    }
    
    @Test
    public void test_LiteralForNullEvalEqualNullValueSouce ()
    {
        assertEquals(LiteralExpression.forNull().evaluation().eval(), NullValueSource.only());
    }
}
