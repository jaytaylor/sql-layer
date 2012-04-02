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
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import junit.framework.Assert;
import org.junit.Test;

public class SqrtExpressionTest extends ComposedExpressionTestBase {
    @Test
    public void testLiteral()
    {
        Expression wholeNumberLiteral = new SqrtExpression(ExprUtil.lit(16.0d));
        Expression fractionalLiteral = new SqrtExpression(ExprUtil.lit(13.33d));
        
        double sqrtWhole = wholeNumberLiteral.evaluation().eval().getDouble();
        double sqrtFrac = fractionalLiteral.evaluation().eval().getDouble();
        
        Assert.assertEquals(4.0d, sqrtWhole, 0.0001);
        Assert.assertEquals(3.65102d, sqrtFrac, 0.0001);
    }
    
    @Test
    public void testNegative()
    {
        Expression negLiteral = new SqrtExpression(ExprUtil.lit(-4.0d));
        ValueSource sqrtOfNegValueSource = negLiteral.evaluation().eval();

        Assert.assertTrue((new Double(sqrtOfNegValueSource.getDouble())).equals(Double.NaN));
    }
    
    @Test
    public void testInfinity()
    {
        Expression infinityLiteral = new SqrtExpression(ExprUtil.lit(Double.POSITIVE_INFINITY));
        double sqrtInfinity = infinityLiteral.evaluation().eval().getDouble();
        
        Assert.assertTrue(Double.isInfinite(sqrtInfinity));
    }

    @Test
    public void testNull()
    {
        Expression nullExpression = new SqrtExpression(ExprUtil.constNull());
        ValueSource sqrtOfNullExpression = nullExpression.evaluation().eval();
        
        Assert.assertEquals("SQRT(NULL) value source should be NullValueSource", 
                NullValueSource.only(), sqrtOfNullExpression);
        Assert.assertEquals("SQRT of NULL should be NULL", 
                AkType.NULL, sqrtOfNullExpression.getConversionType());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return SqrtExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
