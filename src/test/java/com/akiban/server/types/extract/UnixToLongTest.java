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

package com.akiban.server.types.extract;

import java.util.TimeZone;
import com.akiban.server.types.AkType;
import org.junit.Test;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class UnixToLongTest
{
    private final TimeZone defaultTimeZone = TimeZone.getDefault();
    private final String testTimeZone = "UTC";

    @Test
    public void testDate()
    {
        ConverterTestUtils.setGlobalTimezone(testTimeZone);
        long unix = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(1008673L);
        assertEquals(0, unix);

        long stdLong = Extractors.getLongExtractor(AkType.DATE).unixToStdLong(0);
        assertEquals(1008673L, stdLong);
    }

    @Test
    public void testDateTime()
    {
        ConverterTestUtils.setGlobalTimezone(testTimeZone);
        long unix = Extractors.getLongExtractor(AkType.DATETIME).stdLongToUnix(20061107123010L);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(testTimeZone));
        calendar.set(Calendar.YEAR, 2006);
        calendar.set(Calendar.MONTH, 10);
        calendar.set(Calendar.DAY_OF_MONTH, 7);
        calendar.set(Calendar.HOUR_OF_DAY, 12);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 10);
        calendar.set(Calendar.MILLISECOND, 0);
        assertEquals((long)calendar.getTimeInMillis(), unix);

        long stdDate = Extractors.getLongExtractor(AkType.DATETIME).unixToStdLong(unix);
        assertEquals(20061107123010L, stdDate);     
    }

    @Test
    public void testTime()
    {
        ConverterTestUtils.setGlobalTimezone(testTimeZone);
        long stdLong = 123010L;
        long unix = Extractors.getLongExtractor(AkType.TIME).stdLongToUnix(stdLong);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(testTimeZone));
        calendar.set(Calendar.HOUR_OF_DAY, 12);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 10);
        calendar.set(Calendar.MILLISECOND, 0);

        long stdLong1 = Extractors.getLongExtractor(AkType.TIME).unixToStdLong(unix);
        long stdLong2 = Extractors.getLongExtractor(AkType.TIME).unixToStdLong(calendar.getTimeInMillis());

        assertEquals(stdLong, stdLong1);
        assertEquals(stdLong, stdLong2);
    }

    @Test
    public void testYear()
    {
        ConverterTestUtils.setGlobalTimezone(testTimeZone);
        int year = 1991;
        long unix = Extractors.getLongExtractor(AkType.YEAR).stdLongToUnix(year);

        long stdLong1 = Extractors.getLongExtractor(AkType.YEAR).unixToStdLong(unix);
        assertEquals(year, stdLong1);
     
        ConverterTestUtils.setGlobalTimezone(defaultTimeZone.getID());
    }
}
