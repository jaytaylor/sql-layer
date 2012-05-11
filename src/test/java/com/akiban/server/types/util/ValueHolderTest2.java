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

package com.akiban.server.types.util;

import java.util.TimeZone;
import java.util.Calendar;
import com.akiban.server.types.AkType;
import org.junit.Test;
import org.joda.time.DateTime;

import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;

/**
 * Non-parameterised tests for ValueHolder.
 * Mainly to test the conversion between a Joda DateTime() and AkType.DATE/TIME/DATETIME or TIMESTAMP
 *
 */
public class ValueHolderTest2 
{
    @Test
    public void testValueHolderDate ()
    {
        ValueHolder expected = new ValueHolder(AkType.DATE,1008673L ); // JAN - 01 - 1970
        ValueHolder actual = new ValueHolder(AkType.DATE, new DateTime(0, DateTimeZone.UTC));

        assertEquals(expected.getDate(), actual.getDate());
    }

    @Test
    public void testValueHolderTime ()
    {
        ValueHolder expected = new ValueHolder(AkType.TIME, 123010L); // 12:30:10

        Calendar time = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        time.set(Calendar.HOUR_OF_DAY, 12);
        time.set(Calendar.MINUTE, 30);
        time.set(Calendar.SECOND, 10);
        ValueHolder actual = new ValueHolder(AkType.TIME, new DateTime(time.getTimeInMillis(), DateTimeZone.UTC));

        assertEquals(expected.getTime(), actual.getTime());
    }

    @Test
    public void testValueHolderDateTime ()
    {
        ValueHolder expected = new ValueHolder(AkType.DATETIME, 20061107123010L); //2006-11-07 12:30:10
        Calendar datetime = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        datetime.set(Calendar.YEAR, 2006);
        datetime.set(Calendar.MONTH, 10); // month in Calendar is 0-based
        datetime.set(Calendar.DAY_OF_MONTH, 7);
        datetime.set(Calendar.HOUR_OF_DAY, 12);
        datetime.set(Calendar.MINUTE, 30);
        datetime.set(Calendar.SECOND, 10);
        ValueHolder actual = new ValueHolder(AkType.DATETIME, new DateTime(datetime.getTime(), DateTimeZone.UTC));

        assertEquals(expected.getDateTime(), actual.getDateTime());
    }

    @Test
    public void testValueHolderTimestamp ()
    {
        ValueHolder expected = new ValueHolder (AkType.TIMESTAMP,0L); // epoch
        ValueHolder actual = new ValueHolder(AkType.TIMESTAMP, new DateTime(0, DateTimeZone.UTC));

        assertEquals(expected.getTimestamp(), actual.getTimestamp());
    }
     
}
