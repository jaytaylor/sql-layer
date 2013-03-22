
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;
import static org.junit.Assert.*;

public class PeriodDiffExpressionTest extends ComposedExpressionTestBase {
    
    @Test
    public void testYYYYMM_YYYYMM()
    {
        test(120, 201006, 200006);
        test(-120, 200006, 201006);
        test(-1, 199912, 200001);
        test(1, 200001, 199912);
        
        // Low years
        test(10, 95012, 95002);
        test(15, 204, 101);
    }
    
    @Test
    public void testYYMM_YYMM()
    {
        test(120, 1006, 6);
        test(-120, 6, 1006);
        test(-1, 9912, 1); // 199912 - 200001
        test(1, 1, 9912); // 200001 - 199912
        test(0, 2206, 2206);
    }
    
    @Test
    public void testYYYYMM_YYMM()
    {
        test(120, 201006, 6);
        test(-120, 6, 201006);
        test(5, 5007, 5002);
        
        test(-1500 * 12, 50005, 5);
    }
    
    @Test
    public void testLengths()
    {
        test(-69, 123, 456);
        test(-573, 123, 4568);
        test(64, 200505, 1);
        test(-632, 521, 5553);
    }
    
    @Test
    public void testNegative()
    {
        test(-2009 * 12 * 2, -200901, 200901);
        test(0, -123456, -123456);
        test(10, -200912, -201010);
        test(-10, -201010, -200912);
        test(15, -206, -309);
        test(1, -206, -207);
    }
    
    private static void test(long expected, long left, long right)
    {
        Expression testExpr = new PeriodDiffExpression(ExprUtil.lit(left), ExprUtil.lit(right));
        assertEquals(expected, testExpr.evaluation().eval().getLong());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer() {
        return PeriodDiffExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc() {
        return false;
    }
    
}
