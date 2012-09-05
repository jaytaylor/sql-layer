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

package com.akiban.server.types3;

import com.akiban.server.error.InvalidDateFormatException;
import java.util.ArrayList;
import java.util.List;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import org.junit.Test;

import static com.akiban.server.types3.mcompat.mtypes.MDatetimes.*;
import static org.junit.Assert.*;


/**
 * 
 * Test MDatetimes.parseDateOrTime() method
 */
public class MParseDateTimeTest
{
    @Test
    public void parseTimeTest()
    {
        doTest(TIME_ST, "12:30:10",
                        0, 0, 0, 12, 30, 10);
    }
    
    private static void doTest(int expectedType, String st, long...expected)
    {
        long actual[] = new long[6];
        int actualType = MDatetimes.parseDateOrTime(st, actual);
        
        assertEquals("Type: ", name(expectedType),
                               name(actualType));
        
        // convert to lists for a nice error msg, if any
        assertEquals(toList(expected),
                     toList(actual));
        
    }
    
    @Test
    public void parseTimeWithDayTest()
    {
        doTest(TIME_ST, "1 1:1:0",
                        0, 0, 0, 25, 1, 0);
    }

    @Test
    public void parseNegTime()
    {
        doTest(TIME_ST, "-12:0:2",
                        0, 0, 0, -12, 0, 2);
    }
    
    @Test
    public void parseNegTimeWithDay()
    {
        doTest(TIME_ST, "-1 3:4:5",
                        0, 0, 0, -27, 4, 5);
    }
    
    @Test
    public void parseDate()
    {
        doTest(DATE_ST, "2002-12-30",
                        2002, 12, 30, 0, 0, 0);
    }
    
    @Test
    public void parseDateTime()
    {
        doTest(DATETIME_ST, "1900-1-2 12:30:10",
                            1900, 1, 2, 12, 30, 10);
    }
    
    @Test(expected=InvalidDateFormatException.class)
    public void testInvalidTime() // test invalid time in DATETIME
    {
        doTest(DATETIME_ST, "1900-1-2 25:30:10",
                            0, 0, 0, 0, 0, 0);
    }
    
    public void testTime() // 25:30:10 is a valid TIME though
    {
        doTest(TIME_ST, "25:30:10",
                        0, 0, 0, 25, 30, 10);
    }
    
    private static List<Long> toList(long...exp)
    {
        List<Long> list = new ArrayList<Long>(exp.length);
        
        for (long val : exp)
            list.add(val);
        
        return list;
    }
    
    private static String name(int type)
    {
        switch(type)
        {
            case DATETIME_ST:    return "DATETIME_ST";
            case DATE_ST:        return "DATE_ST";
            case TIME_ST:        return "TIME_ST";
            default:             return "UNKNOWN";
        }
    }
}
