/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */


package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;
import static org.junit.Assert.*;

public class SysDateExpressionTest 
{
    @Test
    public void test() throws InterruptedException
    {
        int dt = 4;
        
        ValueHolder date1 = new ValueHolder(new SysDateExpression().evaluation().eval());
        Thread.sleep(dt * 1000);
        
        ValueHolder date2 = new ValueHolder(new SysDateExpression().evaluation().eval());
        
        assertEquals(date1.getDateTime(), date2.getDateTime() - dt); // the first and second call to SysDate() are 4 secs away
    }
    
    @Test
    public void testConst()
    {
        Expression sys = new SysDateExpression();
        assertFalse(sys.isConstant());
    }
    
}
