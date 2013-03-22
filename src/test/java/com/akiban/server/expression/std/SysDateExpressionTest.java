

package com.akiban.server.expression.std;

import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import org.junit.Test;
import static org.junit.Assert.*;

public class SysDateExpressionTest 
{
    @Test
    public void test() throws InterruptedException
    {
        ValueSource date = (new SysDateExpression().evaluation()).eval();
        long dt = System.currentTimeMillis();
        assertEquals("current time in seconds: " + dt,
                (dt)/1000, 
                date.getTimestamp(), 1); // dt and date could be a few millisecs away
     }
    
    @Test
    public void testConst()
    {
        Expression sys = new SysDateExpression();
        assertFalse("SysDateExpression is const" , sys.isConstant());
    }  
}
