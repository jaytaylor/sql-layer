/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression.std;

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import java.util.HashMap;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    
    // Test the parsePeriod function used in both PERIOD_ADD and PERIOD_DIFF
    @Test
    public void testParsePeriod() 
    {
        HashMap<String, Long> dicYYMM = PeriodAddExpression.parsePeriod(610);
        assertEquals(2006, (long) dicYYMM.get("year"));
        assertEquals(10, (long) dicYYMM.get("month"));
        
        HashMap<String, Long> dicLongYear = PeriodAddExpression.parsePeriod(98005);
        assertEquals(980, (long) dicLongYear.get("year"));
        assertEquals(5, (long) dicLongYear.get("month"));
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
