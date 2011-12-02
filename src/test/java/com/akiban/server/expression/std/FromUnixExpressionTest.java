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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 
 * Test for FromUnixExpression
 * 
 * from_unixtime(...) expression is timezone-sensitive, and thus so as the test.
 * The test was written with the assumption that current timezone is GMT +5,
 * hence it'll only pass in this timezone
 */
public class FromUnixExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        // epoch
        test(0L, 19691231190000L); // NOT 1970-01-01 00:00:00 because current timezone isn't UTC
        
        // 2007-11-30 11:30:19
        test(1196440219L, 20071130113019L);
        
        // epoch: unix -> numeric string
        test(0L, "%y-%m-%d", "69-12-31");
        
        // epoch : unix -> string, month name
        test(0L, "%Y-%M-%d", "1969-December-31");
        
        // epoch : unix -> string, month name and day with suffix with leading chars 
        test(0L, "This is epoch in text %Y-%b-%D", "This is epoch in text 1969-Dec-31st");
        
        // 2007-11-30 11:30:19
        test(1196440219L, "%Y %b -%D :: %r", "2007 Nov -30th :: 11:30:19");
        
        // 2007-11-30 11:30:19: unix -> sring with trailing chars
        test(1196440219L, "%D %H display only day of month and hour", "30th 11 display only day of month and hour");
        
        // 2007-11-30 11:30:19: unix -> string with chars in between
        test(1196440219L, "Date part: %d/%m/%Y, Time part: %T", "Date part: 30/11/2007, Time part: 11:30:19");
        
        // 2007-11-30 11:30:19: unix --> string, using % to format output
        test(1196440219L, "%Y%%%M%%%d", "2007%November%30");
        
        // 2007-11-30 11:30:19: unix ---> week string
        test(1196440219L, "%X %V", "2007 47");
        
        // 2007-11-30 11:30:19: unix ---> week string
        test(1196440219L, "%x %v", "2007 48");
        
         // 2007-11-30 11:30:19: unix ---> week string
        test(1196440219L, "%U", "47");
        
        // 2007-11-30 11:30:19: unix ---> week string
        test(1196440219L, "%u", "48");
        
        
        // unknown specifier is treated as regular char
        test(0L, "%z %D", "z 31st");
    }
    
    private static void test (long unix, long expected)
    {
        Expression unixExp = new LiteralExpression(AkType.LONG, unix);
        Expression fromUnix = getTop(unixExp, null);
        
        if (expected < 0)
            assertTrue (fromUnix.evaluation().eval().isNull());
        else
            assertEquals(expected, fromUnix.evaluation().eval().getDateTime());
    }
    
    private static void test (long unix, String format, String expected)
    {
        Expression unixExp = new LiteralExpression(AkType.LONG, unix);
        Expression fmExp = new LiteralExpression(AkType.VARCHAR, format);
        
        Expression top = getTop(unixExp, fmExp);
        if (expected == null)
            assertTrue(top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
    }

    private static Expression getTop(Expression unix, Expression st)
    {
        if (st == null)
            return new FromUnixExpression(Arrays.asList(unix));
        else return new FromUnixExpression(Arrays.asList(unix, st));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return new CompositionTestInfo(1, AkType.LONG,true);
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return FromUnixExpression.COMPOSER;
    }
}
