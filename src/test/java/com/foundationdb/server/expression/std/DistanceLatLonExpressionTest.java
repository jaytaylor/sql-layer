/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.OutOfRangeException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
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
