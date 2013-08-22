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

import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.expression.Expression;
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
