
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class RandExpressionTest
{
    
    private static List<Double> getRand (long seed, int size)
    {
        List<Double> ret = new ArrayList<>(size);
        Expression input = new LiteralExpression(AkType.LONG, seed);
        Expression top = new RandExpression(Arrays.asList(input));
        
        for (int n = 0; n < size; ++n)
            ret.add(top.evaluation().eval().getDouble());
        return ret;
    }
    
    private static void doTest(long seed, int size)
    {       
        List<Double> l1 = getRand(seed, size);
        List<Double> l2 = getRand(seed, size);
        
        for (int n = 0; n < size; ++n)
            assertEquals(l1.get(n), l2.get(n), 0.0001);
    }
    
    @Test
    public void test()
    {
        doTest(3, 5);
        doTest(1,10);
        doTest(6,2);
    }
}
