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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class MDatetimes {

    private static final TBundleID bundle = MBundle.INSTANCE.id();
    
    public static final NoAttrTClass DATE = new NoAttrTClass(bundle,
            "date", 1, 1, 4, PUnderlying.INT_32);
    public static final NoAttrTClass DATETIME = new NoAttrTClass(bundle,
            "datetime", 1, 1, 8, PUnderlying.INT_64);
    public static final NoAttrTClass TIME = new NoAttrTClass(bundle,
            "time", 1, 1, 4, PUnderlying.INT_32);
    public static final NoAttrTClass YEAR = new NoAttrTClass(bundle,
            "year", 1, 1, 1, PUnderlying.INT_8);
    public static final NoAttrTClass TIMESTAMP = new NoAttrTClass(bundle,
            "timestamp", 1, 1, 4, PUnderlying.INT_32);
    
    public static MutableDateTime toJodaDatetime(long ymd_hms[], String tz)
    {
        return new MutableDateTime((int)ymd_hms[YEAR_INDEX], (int)ymd_hms[MONTH_INDEX], (int)ymd_hms[DAY_INDEX],
                            (int)ymd_hms[HOUR_INDEX], (int)ymd_hms[MIN_INDEX], (int)ymd_hms[SEC_INDEX], 0,
                            DateTimeZone.forID(tz));
    }
    
    public static long[] fromJodaDatetime (MutableDateTime date)
    {
        return new long[]
        {
            date.getYear(),
            date.getMonthOfYear(),
            date.getDayOfMonth(),
            date.getHourOfDay(),
            date.getMinuteOfHour(),
            date.getSecondOfMinute()
        };
    }
    
    public static long[] fromJodaDatetime (DateTime date)
    {
        return new long[]
        {
            date.getYear(),
            date.getMonthOfYear(),
            date.getDayOfMonth(),
            date.getHourOfDay(),
            date.getMinuteOfHour(),
            date.getSecondOfMinute()
        };
    }

    public static long[] fromEncodedDate(long val)
    {
        return new long[]
        {
            val / 512,
            val / 32 % 16,
            val % 32
        };
    }
    
    public static int encodeDate (long ymd[])
    {
        return (int)(ymd[YEAR_INDEX] * 512 + ymd[MONTH_INDEX] * 32 + ymd[DAY_INDEX]);
    }
    
    public static long[] fromDate(long val)
    {
        return new long[]
        {
            val / DATE_YEAR,
            val / DATE_MONTH % DATE_MONTH,
            val % DATE_MONTH,
            0,
            0,
            0
        };
    }
    
    public static long[] fromDatetime (long val)
    {
        return new long[]
        {
            val / DATETIME_YEAR_SCALE,
            val / DATETIME_MONTH_SCALE % 100,
            val / DATETIME_DAY_SCALE % 100,
            val / DATETIME_HOUR_SCALE % 100,
            val / DATETIME_MIN_SCALE % 100,
            val % 100
        };
    }

    public static long encodeDatetime(long ymd[])
    {
        return ymd[YEAR_INDEX] * DATETIME_YEAR_SCALE 
                + ymd[MONTH_INDEX] * DATETIME_MONTH_SCALE
                + ymd[DAY_INDEX] * DATETIME_DAY_SCALE
                + ymd[HOUR_INDEX] * DATETIME_HOUR_SCALE
                + ymd[MIN_INDEX] * DATETIME_MIN_SCALE
                + ymd[SEC_INDEX];
    }

    public static long[] fromTime(long val)
    {
        return new long[]
        {
            1970,
            1,
            1,
            val / DATETIME_HOUR_SCALE,
            val / DATETIME_MIN_SCALE % 100,
            val % 100
        };
    }
    
    public static long toTime(long val[])
    {
        return val[HOUR_INDEX] * DATETIME_HOUR_SCALE
                + val[MIN_INDEX] * DATETIME_MIN_SCALE
                + val[SEC_INDEX];
    }
    public static boolean isValidDatetime (long ymdhms[])
    {
        return isValidDayMonth(ymdhms) && isValidHrMinSec(ymdhms);
    }
    
    public static boolean isValidHrMinSec (long ymdhms[])
    {
        return ymdhms[HOUR_INDEX] >= 0 && ymdhms[HOUR_INDEX] < 24 
                && ymdhms[MIN_INDEX] >= 0 && ymdhms[MIN_INDEX] < 60 
                && ymdhms[SEC_INDEX] >= 0 && ymdhms[SEC_INDEX] < 60;
    }
 
    private static boolean isValidDayMonth(long ymd[])
    {
        long last = getLastDay(ymd);
        return last > 0 && ymd[DAY_INDEX] <= last;
    }
        
    protected static long getLastDay(long ymd[])
    {
        switch ((int) ymd[1])
        {
            case 2:
                return ymd[0] % 400 == 0 || ymd[0] % 4 == 0 && ymd[0] % 100 != 0 ? 29L : 28L;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30L;
            case 3:
            case 1:
            case 5:
            case 7:
            case 8:
            case 10:
            case 0:
            case 12:
                return 31L;
            default:
                return -1;
        }
    }

    public static final int YEAR_INDEX = 0;
    public static final int MONTH_INDEX = 1;
    public static final int DAY_INDEX = 2;
    public static final int HOUR_INDEX = 3;
    public static final int MIN_INDEX = 4;
    public static final int SEC_INDEX = 5;
    
    private static final int DATE_YEAR = 10000;
    private static final int DATE_MONTH = 100;

    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
}
