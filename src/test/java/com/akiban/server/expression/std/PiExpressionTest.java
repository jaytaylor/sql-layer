
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.TrigExpression.*;

public class PiExpressionTest 
{
    @Test
    public void test()
    {
        test(COS_COMPOSER, -1.0);
        test(SIN_COMPOSER, 0.0);
        test(TAN_COMPOSER, 0.0);
        test(COT_COMPOSER, Math.cos(Math.PI) / Math.sin(Math.PI));
    }
    
    public void test (ExpressionComposer c, double expected)
    {
        assertEquals( c.toString() + "(PI()) ", 
                expected, 
                      c.compose(Arrays.asList(PiExpression.COMPOSER.compose(new ArrayList<Expression>(), Collections.<ExpressionType>nCopies(1, ExpressionTypes.DOUBLE))), Collections.<ExpressionType>nCopies(2, ExpressionTypes.DOUBLE)).evaluation().eval().getDouble(), 
                0.0001);
    }
}
