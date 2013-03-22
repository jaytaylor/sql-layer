
package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.DateTimeZone;

public abstract class LongExtractor extends AbstractExtractor {

    // LongExtractor interface

    public abstract long getLong(ValueSource source);
    public abstract String asString(long value);
    public abstract long getLong(String string);

    // for date/times only
    public abstract long stdLongToUnix (long longVal);
    public abstract long unixToStdLong (long unixVal);
    public abstract long[] getYearMonthDayHourMinuteSecond (long value);
    public abstract long getEncoded (long ymd_hms[]);
    
    public abstract long stdLongToUnix (long longVal, DateTimeZone tz);
    public abstract long unixToStdLong (long unixVal, DateTimeZone tz);
    public abstract long[] getYearMonthDayHourMinuteSecond (long value, DateTimeZone tz);
    public abstract long getEncoded (long ymd_hms[], DateTimeZone tz);
    
    // package-private ctor
    LongExtractor(AkType targetConversionType) {
        super(targetConversionType);
    }
}
