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
