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

import org.joda.time.DateTimeZone;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import java.util.TimeZone;
import org.junit.Test;

import static org.junit.Assert.*;

public class FromUnixExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        DateTimeZone defaultTimezone = DateTimeZone.getDefault();
        // set time zone to UTC for testing
        DateTimeZone testingTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(testingTimeZone);

        // epoch
        test(0L, 19700101000000L);

        // 2007-11-30 16:30:19
        test(1196440219L, 20071130163019L);

        // epoch: unix -> numeric string
        test(0L, "%y-%m-%d", "70-01-01");

        // epoch : unix -> string, month name
        test(0L, "%Y-%M-%d", "1970-January-01");

        // epoch : unix -> string, month name and day with suffix with leading chars
        test(0L, "This is epoch in text %Y-%b-%D", "This is epoch in text 1970-Jan-1st");

        // 2007-11-30 16:30:19
        test(1196440219L, "%Y %b -%D :: %r", "2007 Nov -30th :: 04:30:19 PM");

        // 2007-11-30 16:30:19: unix -> sring with trailing chars
        test(1196440219L, "%D %H display only day of month and hour", "30th 16 display only day of month and hour");

        // 2007-11-30 16:30:19: unix -> string with chars in between
        test(1196440219L, "Date part: %d/%m/%Y, Time part: %T", "Date part: 30/11/2007, Time part: 16:30:19");

        // 2007-11-30 16:30:19: unix --> string, using % to format output
        test(1196440219L, "%Y%%%M%%%d", "2007%November%30");

        // 2007-11-30 16:30:19: unix ---> week string
        test(1196440219L, "%X %V", "2007 47");

        // 2007-11-30 16:30:19: unix ---> week string
        test(1196440219L, "%x %v", "2007 48");

         // 2007-11-30 16:30:19: unix ---> week string
        test(1196440219L, "%U", "47");

        // 2007-11-30 16:30:19: unix ---> week string
        test(1196440219L, "%u", "48");


        // unknown specifier is treated as regular char
        test(0L, "%z %D", "z 1st");

        // reset time zone to default
        DateTimeZone.setDefault(defaultTimezone);
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

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
