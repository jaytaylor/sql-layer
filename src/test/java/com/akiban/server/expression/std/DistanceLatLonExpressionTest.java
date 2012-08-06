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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;

public class DistanceLatLonExpressionTest extends ComposedExpressionTestBase {

    @Test
    public void testZero() {
        Expression intZero = getInt(0);
        Expression doubleZero = getDouble(0.0);
        Expression longZero = getLong(0);
        
        List<Expression> intLst = new LinkedList<Expression>(Arrays.asList(intZero, intZero, intZero, intZero));
        List<Expression> doubleLst = new LinkedList<Expression>(Arrays.asList(doubleZero, doubleZero, doubleZero, doubleZero));
        List<Expression> longLst = new LinkedList<Expression>(Arrays.asList(longZero, longZero, longZero, longZero));
        
        test(0.0, intLst);
        test(0.0, doubleLst);
        test(0.0, longLst);
    }
    
    @Test
    public void testPosNoWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDouble(90), getDouble(0), getDouble(100), getDouble(0)));
        test(10.0, lst);
    }
    
    @Test
    public void testPosWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDouble(0), getDouble(0), getDouble(180), getDouble(0)));
        test(180.0, lst);
    }
    
    @Test
    public void testNegNoWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDouble(90), getDouble(-90), getDouble(100), getDouble(-90)));
        test(10.0, lst);
    }
    
    @Test
    public void testNegWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDouble(-179), getDouble(0), getDouble(180), getDouble(0)));
        test(1.0, lst);
    }
    
    @Test
    public void testNaN()
    {
        Expression exp = getDouble(Double.NaN);
        Expression num = getDouble(12.0);
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(exp, num, num, num));
        
        Expression top = new DistanceLatLonExpression(lst);
        double nan = top.evaluation().eval().getDouble();

        Assert.assertTrue(Double.isNaN(nan));
    }
    
    @Test (expected=WrongExpressionArityException.class)
    public void testArity() {
        compose(DistanceLatLonExpression.COMPOSER, Arrays.asList(LiteralExpression.forNull()));
    }

    
    private void test(double expected, List<Expression> lst) {
        Expression exp = new DistanceLatLonExpression(lst);
        
        double result = exp.evaluation().eval().getDouble();
        Assert.assertEquals(expected, result);
    }
    
    private LiteralExpression getDouble(double num) {
        return new LiteralExpression(AkType.DOUBLE, num);
    }
    
    private LiteralExpression getLong(long num) {
        return new LiteralExpression(AkType.LONG, num);
    }
    
    private LiteralExpression getInt(int num) {
        return new LiteralExpression(AkType.INT, num);
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() {
        return new CompositionTestInfo(1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer() {
        return DistanceLatLonExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc() {
        return false;
    }

}
