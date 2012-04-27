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

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.joda.time.DateTimeZone;

class ExtractorsForLong extends LongExtractor {

    static final LongExtractor LONG = new ExtractorsForLong(AkType.LONG) ;
    static final LongExtractor INT = new ExtractorsForLong(AkType.INT);
    static final LongExtractor U_INT = new ExtractorsForLong(AkType.U_INT);

    @Override
    public String asString(long value) {
        return Long.toString(value);
    }

    @Override
    public long getLong(String string) {
        try {
            return Long.parseLong(string);
        } catch (NumberFormatException ex) {
            throw new InvalidCharToNumException (string); 
        }
    }

    @Override
    public long getLong(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();
        AkType type = source.getConversionType();
        switch (type) {
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case U_BIGINT:  return source.getUBigInt().longValue();
        case FLOAT:     return Math.round(source.getFloat());
        case U_FLOAT:   return Math.round(source.getUFloat());
        case DOUBLE:    return Math.round(source.getDouble());
        case U_DOUBLE:  return Math.round(source.getUDouble());
        case TEXT:
            try {
                return Math.round(Double.parseDouble(source.getText()));    
            } catch (NumberFormatException ex) {
                throw new InvalidCharToNumException (source.getText());
            }
        case VARCHAR:
            try {
                return Math.round(Double.parseDouble(source.getString()));
            } catch (NumberFormatException ex) {
                throw new InvalidCharToNumException (source.getString());
            }
        case DECIMAL:   return source.getDecimal().setScale(0, RoundingMode.HALF_UP).longValue();
        case DATE:      return source.getDate();
        case DATETIME:  return source.getDateTime();
        case TIME:      return source.getTime();
        case TIMESTAMP: return source.getTimestamp();
        case YEAR:      return source.getYear();
        case INTERVAL_MILLIS:  return source.getInterval_Millis();
        case INTERVAL_MONTH:  return source.getInterval_Month();
        default: throw unsupportedConversion(type);
        }
    }

    private ExtractorsForLong(AkType targetConversionType) {
        super(targetConversionType);
    }

    @Override
    public long stdLongToUnix(long longVal) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long unixToStdLong(long unixVal) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }
    
    @Override
    public long[] getYearMonthDayHourMinuteSecond (long value) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }
    
    @Override
    public long getEncoded (long [] ymd_hms) {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long stdLongToUnix(long longVal, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long unixToStdLong(long unixVal, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long[] getYearMonthDayHourMinuteSecond(long value, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Unsupported! Only works for date/time types");
    }

    @Override
    public long getEncoded(long[] ymd_hms, DateTimeZone tz)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
