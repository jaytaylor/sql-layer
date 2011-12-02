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
