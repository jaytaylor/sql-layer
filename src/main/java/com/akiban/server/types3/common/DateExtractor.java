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
package com.akiban.server.types3.common;

import com.akiban.server.types3.TExecutionContext;
import org.joda.time.MutableDateTime;

public class DateExtractor {

    // consts
    private static final long DATETIME_DATE_SCALE = 1000000L;
    private static final long DATETIME_YEAR_SCALE = 10000L * DATETIME_DATE_SCALE;
    private static final long DATETIME_MONTH_SCALE = 100L * DATETIME_DATE_SCALE;
    private static final long DATETIME_DAY_SCALE = 1L * DATETIME_DATE_SCALE;
    private static final long DATETIME_HOUR_SCALE = 10000L;
    private static final long DATETIME_MIN_SCALE = 100L;
    private static final long DATETIME_SEC_SCALE = 1L;
    
    private static final long DATE_YEAR_SCALE = 1000000L;
    private static final long DATE_MONTH_SCALE = 100L;
    private static final long DATE_DAY_SCALE = 1L;
    
    public static final int YEAR = 0;
    public static final int MONTH = 1; 
    public static final int DAY = 2;
    public static final int HOUR = 3;
    public static final int MINUTE = 4;
    public static final int SECOND = 5;
    
    private static final int DATE_INDEX = 0;   
    public static final long BEGINNING = new MutableDateTime(0,0,1,0,0,0,0).getMillis();
    public static final long DAY_FACTOR = 3600L * 1000 * 24;
    
    public static long[] extract(long value) {
        final long year = (value / DATETIME_YEAR_SCALE);
        final long month = (value / DATETIME_MONTH_SCALE) % 100;
        final long day = (value / DATETIME_DAY_SCALE) % 100;
        long hour = value / DATETIME_HOUR_SCALE % 100;
        long minute = value / DATETIME_MIN_SCALE % 100;
        long second = value / DATETIME_SEC_SCALE % 100;
        return new long[]{year, month, day, hour, minute, second};
    }

    public static boolean validHrMinSec(long[] hms) {
        return hms[3] >= 0 && hms[3] < 24 && hms[4] >= 0 && hms[4] < 60 && hms[5] >= 0 && hms[5] < 60;
    }

    public static long getLastDay(long ymd[]) {
        switch ((int) ymd[1]) {
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
                return -1L;
        }
    }

    public static boolean validDayMonth(long[] datetime) {
        long last = getLastDay(datetime);
        return last != -1L && datetime[2] <= last;
    }

    public static MutableDateTime getMutableDateTime(TExecutionContext context, long[] dateArr, boolean setDateTime) {
        MutableDateTime datetime = (MutableDateTime) context.exectimeObjectAt(DATE_INDEX);
        if (context == null) {
            context.putExectimeObject(DATE_INDEX, datetime = new MutableDateTime());
        }

        if (setDateTime) {
            datetime.setDateTime((int) dateArr[DateExtractor.YEAR], (int) dateArr[DateExtractor.MONTH], (int) dateArr[DateExtractor.DAY],
                    (int) dateArr[DateExtractor.HOUR], (int) dateArr[DateExtractor.MINUTE], (int) dateArr[DateExtractor.SECOND], 0);
        }
        return datetime;
    }

    public static int toTime(long hour, long min, long sec) {
        return (int) (hour * DATETIME_HOUR_SCALE + min * DATETIME_MIN_SCALE + sec * DATETIME_SEC_SCALE);
    }
    
    public static long toDate(long year, long month, long day) {
        return year * DATE_YEAR_SCALE + month * DATE_MONTH_SCALE + day * DATE_DAY_SCALE;
    }

    public static long toDatetime(long year, long month, long day, long hour, long minute, long second) {
        return year*DATETIME_YEAR_SCALE + month*DATETIME_MONTH_SCALE + day*DATETIME_DAY_SCALE +
                hour*DATETIME_HOUR_SCALE + minute*DATETIME_MIN_SCALE + second*DATETIME_SEC_SCALE;
    }
}
