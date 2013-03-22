
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import org.junit.Test;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import static org.junit.Assert.*;

public class TodaysExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        doTest("0001-01-01", 366L);
        doTest("0000-01-01", 0L);
        doTest("0000-01-02", 1L);
        doTest("0000-00-00", null);
    }
  
    
    private void doTest (String date, Long expected)
    {
        Expression top = compose(ToDaysExpression.COMPOSER, Arrays.asList( date == null
                ? LiteralExpression.forNull()
                : new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong(date))));
        
        String name = "TO_DAYS(" + date + ") ";
        if (expected == null)
            assertTrue(name + " TOP should be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(name, expected.longValue(), top.evaluation().eval().getLong());
    }
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ToDaysExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
