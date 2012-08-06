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
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;

public class DistanceLatLonExpressionTest {

    @Test
    public void testZero() {
        Expression zero = new LiteralExpression(AkType.DECIMAL, BigDecimal.ZERO);
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(zero, zero, zero, zero));
        
        test(0.0, lst);
    }
    
    @Test
    public void testPosNoWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(90), getDecimal(0), getDecimal(100)));
        test(10.0, lst);
    }
    
    @Test
    public void testPosWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(0), getDecimal(0), getDecimal(180)));
        test(180.0, lst);
    }
    
    @Test
    public void testNegNoWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(-90), getDecimal(90), getDecimal(-90), getDecimal(100)));
        test(10.0, lst);
    }
    
    @Test
    public void testNegWrap() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(-179), getDecimal(0), getDecimal(180)));
        test(1.0, lst);
    }
    
    @Test (expected=WrongExpressionArityException.class)
    public void testArity() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0)));
        Expression exp = new DistanceLatLonExpression(lst);
    }
    
    @Test 
    public void testWrapAround() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(-1), getDecimal(0), getDecimal(1)));
        test(2.0, lst);
    }

    
    private void test(double expected, List<Expression> lst) {
        Expression exp = new DistanceLatLonExpression(lst);
        
        ValueSource result = exp.evaluation().eval();
        Assert.assertEquals(expected, result.getDouble());
    }
    
    private LiteralExpression getDecimal(int num) {
        return new LiteralExpression(AkType.DECIMAL, new BigDecimal(num));
    }
}
