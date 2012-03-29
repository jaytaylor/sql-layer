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

package com.akiban.server.types.conversion.util;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
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
