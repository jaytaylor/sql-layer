
package com.akiban.server.expression.std;

import com.akiban.server.error.OutOfRangeException;
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
        List<Expression> lst = new LinkedList<>(Arrays.asList(zero, zero, zero, zero));
        
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
    
    @Test
    public void testNegPosWrapAround() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(-179), getDecimal(0), getDecimal(180)));
        test(1.0, lst);
    }
    
    @Test (expected=OutOfRangeException.class) 
    public void testRangeTooLargeY() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(91), getDecimal(0), getDecimal(0), getDecimal(180)));
        Expression exp = new DistanceLatLonExpression(lst);
        ValueSource result = exp.evaluation().eval();
    }
    
    @Test (expected=OutOfRangeException.class) 
    public void testRangeTooSmallY() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(-91), getDecimal(0), getDecimal(0), getDecimal(180)));
        Expression exp = new DistanceLatLonExpression(lst);
        ValueSource result = exp.evaluation().eval();
    }
    
    @Test (expected=OutOfRangeException.class) 
    public void testRangeTooLargeX() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(181), getDecimal(0), getDecimal(180)));
        Expression exp = new DistanceLatLonExpression(lst);
        ValueSource result = exp.evaluation().eval();
    }
    
    @Test (expected=OutOfRangeException.class) 
    public void testRangeTooSmallX() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(-181), getDecimal(0), getDecimal(180)));
        Expression exp = new DistanceLatLonExpression(lst);
        ValueSource result = exp.evaluation().eval();
    }
    
    @Test
    public void testSameLon() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(80), getDecimal(10), getDecimal(80)));
        test(10.0, lst);
    }
    
    @Test
    public void testVy1() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(180), getDecimal(0), getDecimal(-180)));
        test(0.0, lst); 
    }
    
    @Test
    public void testVy2() {
        List<Expression> lst = new LinkedList<Expression>(Arrays.asList(getDecimal(0), getDecimal(179), getDecimal(0), getDecimal(-180)));
        test(1.0, lst);
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
