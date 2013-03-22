
package com.akiban.server.types.conversion.util;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

class DateTimeConverter implements AbstractConverter<MutableDateTime>
{
    @Override
    public  MutableDateTime get(ValueSource source)
    {
        if (source.isNull()) return null;
        long [] ymd_hms;
        switch(source.getConversionType())
        {
            case DATE:      ymd_hms = Extractors.getLongExtractor(AkType.DATE).
                                getYearMonthDayHourMinuteSecond(source.getDate());
                            checkArgs(ymd_hms);
                            break;
            case DATETIME:  ymd_hms = Extractors.getLongExtractor(AkType.DATETIME).
                                getYearMonthDayHourMinuteSecond(source.getDateTime());
                            checkArgs(ymd_hms);
                            break;
            case TIMESTAMP: return new MutableDateTime(source.getTimestamp() * 1000);
            case TIME:      ymd_hms =  Extractors.getLongExtractor(AkType.TIME).
                                getYearMonthDayHourMinuteSecond(source.getTime()); break;
            case VARCHAR:   return get(source.getString());
            case TEXT:      return get(source.getText());
            case YEAR:      throw new InvalidParameterValueException("Invalid Date Value");
            default:        throw new InconvertibleTypesException(source.getConversionType(),
                                                                    AkType.DATETIME);
        }
        return new MutableDateTime((int)ymd_hms[0], (int)ymd_hms[1], (int)ymd_hms[2],
                (int)ymd_hms[3], (int)ymd_hms[4], (int)ymd_hms[5], 0, DateTimeZone.getDefault());
    }

    private static void checkArgs (long [] ymd_hms)
    {
        if (ymd_hms[0] * ymd_hms[1] * ymd_hms[2] == 0) throw new InvalidParameterValueException();
    }

    @Override
    public MutableDateTime get(String source)
    {
        return MutableDateTime.parse(source);
    }

    protected DateTimeConverter () {}
}
