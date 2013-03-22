
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;
import static org.junit.Assert.*;

public class PeriodAddExpressionTest extends ComposedExpressionTestBase {

    @Test
    public void testPositive()
    {
        Expression sameYear = new PeriodAddExpression(ExprUtil.lit(201201), ExprUtil.lit(5));
        assertEquals(201206, sameYear.evaluation().eval().getLong());

        Expression newYear = new PeriodAddExpression(ExprUtil.lit(201206), ExprUtil.lit(15));
        assertEquals(201309, newYear.evaluation().eval().getLong());
        
        Expression largeOffset = new PeriodAddExpression(ExprUtil.lit(199502), ExprUtil.lit(124));
        assertEquals(200506, largeOffset.evaluation().eval().getLong());
        
        // YYMM format
        Expression addYYMM = new PeriodAddExpression(ExprUtil.lit(502), ExprUtil.lit(10));
        assertEquals(200512, addYYMM.evaluation().eval().getLong());
        
    }
    
    @Test
    public void testNegative()
    {
        Expression sameYear = new PeriodAddExpression(ExprUtil.lit(201209), ExprUtil.lit(-5));
        assertEquals(201204, sameYear.evaluation().eval().getLong());

        Expression newYear = new PeriodAddExpression(ExprUtil.lit(201206), ExprUtil.lit(-25));
        assertEquals(201005, newYear.evaluation().eval().getLong());
        
        Expression largeOffset = new PeriodAddExpression(ExprUtil.lit(200501), ExprUtil.lit(-121));
        assertEquals(199412, largeOffset.evaluation().eval().getLong());    
        
        // YYMM format
        Expression addYYMM = new PeriodAddExpression(ExprUtil.lit(5002), ExprUtil.lit(-11));
        assertEquals(204903, addYYMM.evaluation().eval().getLong());
    }
    
    @Test
    public void testVaryingLengths()
    {
        // These match MySQL's output
        test(200012, 100, 0);
        test(200002, 1, 1);
        test(200712, 95, 1);
        test(201404, 951, 13);
        test(203404, 2951, 13);
        test(54511, 54321, 14);
        test(954412, 954321, 3);
    }
    
    @Test
    public void testGreaterThan12Month()
    {
        test(199705, 199415, 26);
        test(20407, 19941, 26);
    }
    
    @Test
    public void testZeroMonth()
    {
        test(200012, 100, 0);
        test(200012, 200000, 12);
    }

    @Test
    public void testNegativePeriod()
    {
        test(-200012, -200006, -6);
        test(-200012, -6, -6);
        test(-200012, -200101, 1);
        test(-200012, -101, 1);
        test(-200412, -502, 2);
        test(-199812, -12, 24);
        test(-100006, 99912, -24006);
        test(100006, -99912, 24006);
    }
    
    private void test(long expected, long period, long offset)
    {
        Expression testExpr = new PeriodAddExpression(ExprUtil.lit(period), ExprUtil.lit(offset));
        assertEquals(expected, testExpr.evaluation().eval().getLong());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return new CompositionTestInfo(2, AkType.LONG, true);
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return PeriodAddExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc() 
    {
        return false;
    }
}
