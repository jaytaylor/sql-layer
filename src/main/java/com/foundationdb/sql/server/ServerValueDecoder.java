/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.server;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.math.BigDecimal;
import java.io.*;

/** Decode values from external representation into query bindings. */
public class ServerValueDecoder
{
    private String encoding;

    public ServerValueDecoder(String encoding) {
        this.encoding = encoding;
    }

    /** Decode the given value into a the given bindings at the given position.
     */
    public void decodeValue(byte[] encoded, ServerType type, boolean binary,
                            QueryBindings bindings, int index) {
       
        TInstance targetType = type != null ? type.getInstance() : null;
        if (targetType == null)
            targetType = MString.varchar();
        Object value;
        if (encoded == null) {
            value = null;
        }
        else if (!binary) {
            try {
                value = new String(encoded, encoding);
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException("", "", encoding);
            }
        }
        else {
            try {
                switch (type.getBinaryEncoding()) {
                case BINARY_OCTAL_TEXT:
                default:
                    if (targetType.typeClass() instanceof MString)
                        value = new String(encoded, encoding);
                    else
                        value = encoded;
                    break;
                case INT_8:
                case INT_16:
                case INT_32:
                case INT_64:
                    // Go by the length sent rather than the implied type.
                    switch (encoded.length) {
                    case 1:
                        value = (long)getDataStream(encoded).read();
                        break;
                    case 2:
                        value = (long)getDataStream(encoded).readShort();
                        break;
                    case 4:
                        value = (long)getDataStream(encoded).readInt();
                        break;
                    case 8:
                        value = getDataStream(encoded).readLong();
                        break;
                    default:
                        throw new AkibanInternalException("Not an integer size: " + encoded);
                    }
                    if (targetType.typeClass() == MDatetimes.YEAR) {
                        value = ((Long)value) - 1900;
                    }
                    break;
                case FLOAT_32:
                    value = getDataStream(encoded).readFloat();
                    break;
                case FLOAT_64:
                    value = getDataStream(encoded).readDouble();
                    break;
                case STRING_BYTES:
                    value = new String(encoded, encoding);
                    break;
                case BOOLEAN_C:
                    value = (encoded[0] != 0);
                    break;
                case TIMESTAMP_FLOAT64_SECS_2000_NOTZ:
                    value = valueForUnixtime(seconds2000NoTZ((int)getDataStream(encoded).readDouble()),
                                             targetType);
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                    value = valueForUnixtime(seconds2000NoTZ((int)(getDataStream(encoded).readLong() / 1000000L)),
                                             targetType);
                    break;
                case DAYS_2000:
                    value = days2000(getDataStream(encoded).readInt());
                    break;
                case TIME_FLOAT64_SECS_NOTZ:
                    value = timeSecsNoTZ((int)getDataStream(encoded).readDouble());
                    break;
                case TIME_INT64_MICROS_NOTZ:
                    value = timeSecsNoTZ((int)(getDataStream(encoded).readLong() / 1000000L));
                    break;
                case DECIMAL_PG_NUMERIC_VAR:
                    {
                        DataInputStream dstr = getDataStream(encoded);
                        short ndigits = dstr.readShort();
                        short[] digits = new short[ndigits + 4];
                        digits[0] = ndigits;
                        for (int i = 1; i < digits.length; i++) {
                            digits[i] = dstr.readShort();
                        }
                        value = pgNumericVar(digits);
                    }
                    break;
                }
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException("", "", encoding);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("IO error reading from byte array", ex);
            }
        }
        ValueSource source = ValueSources.valuefromObject(value, targetType);
        bindings.setValue(index, source);
    }

    private static DataInputStream getDataStream(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static Object valueForUnixtime(int unixtime, TInstance type) {
        if (type.typeClass() == MDatetimes.DATETIME) {
            DateTime dt = new DateTime(unixtime * 1000L);
            return (dt.getYear() * 10000000000L +
                    dt.getMonthOfYear() * 100000000 +
                    dt.getDayOfMonth() * 1000000 +
                    dt.getHourOfDay() * 10000 +
                    dt.getMinuteOfHour() * 100 +
                    dt.getSecondOfMinute());
        }
        return unixtime;
    }

    private static int seconds2000NoTZ(int secs) {
        int unixtime = secs + 946702800; // 2000-01-01 00:00:00-UTC.
        DateTimeZone dtz = DateTimeZone.getDefault();
        unixtime -= (dtz.getOffset(unixtime * 1000L) - dtz.getStandardOffset(unixtime * 1000L)) / 1000;
        return unixtime;
    }

    private static int days2000(int days) {
        int unixtime = seconds2000NoTZ(days * 86400);
        DateTime dt = new DateTime(unixtime * 1000L);
        return (dt.getYear() * 512 + 
                dt.getMonthOfYear() * 32 + 
                dt.getDayOfMonth());
    }

    private static int timeSecsNoTZ(int secs) {
        int h = secs / 3600;
        int m = (secs / 60) % 60;
        int s = secs % 60;
        return (h * 10000 + m * 100 + s);
    }

    private static final short NUMERIC_POS = 0x0000;
    private static final short NUMERIC_NEG = 0x4000;
    private static final short NUMERIC_NAN = (short)0xC000;

    private static BigDecimal pgNumericVar(short[] digits) {
        short ndigits, weight, sign, dscale;
        ndigits = digits[0];
        weight = digits[1];
        sign = digits[2];
        dscale = digits[3];
        BigDecimal result = BigDecimal.ZERO;
        for (int i = 0; i < ndigits; i++) {
            BigDecimal digit = new BigDecimal(digits[i + 4]);
            result = result.add(digit.scaleByPowerOfTen((weight - i) * 4));
        }
        if (sign == NUMERIC_NEG)
            result = result.negate();
        return result;
    }

}
