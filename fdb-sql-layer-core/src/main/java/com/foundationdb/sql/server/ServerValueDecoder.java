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
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collections;
import java.io.*;

/** Decode values from external representation into query bindings. */
public class ServerValueDecoder
{
    private final TypesTranslator typesTranslator;
    private final String encoding;

    public ServerValueDecoder(TypesTranslator typesTranslator, String encoding) {
        this.typesTranslator = typesTranslator;
        this.encoding = encoding;
    }

    /** Decode the given value into a the given bindings at the given position.
     */
    public void decodeValue(byte[] encoded, ServerType type, boolean binary,
                            QueryBindings bindings, int index,
                            QueryContext queryContext) {
       
        TInstance targetType = type != null ? type.getType() : null;
        if (targetType == null)
            targetType = typesTranslator.typeForString();
        Object value = null;
        long lvalue = 0;
        int nanos = 0;
        int lvalueType = Types.NULL;
        if (encoded == null) {
            value = null;
        }
        else if (!binary) {
            try {
                value = new String(encoded, encoding);
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException(encoding);
            }
        }
        else {
            try {
                switch (type.getBinaryEncoding()) {
                case BINARY_OCTAL_TEXT:
                default:
                    if (targetType.typeClass() instanceof TString)
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
                        lvalue = getDataStream(encoded).read();
                        break;
                    case 2:
                        lvalue = getDataStream(encoded).readShort();
                        break;
                    case 4:
                        lvalue = getDataStream(encoded).readInt();
                        break;
                    case 8:
                        lvalue = getDataStream(encoded).readLong();
                        break;
                    default:
                        throw new AkibanInternalException("Not an integer size: " + encoded);
                    }
                    lvalueType = Types.BIGINT;
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
                    {
                        double dsecs = getDataStream(encoded).readDouble();
                        long secs = (long)dsecs;
                        lvalue = seconds2000NoTZ(secs);
                        nanos = (int)((dsecs - secs) * 1.0e9);
                        lvalueType = Types.TIMESTAMP;
                    }
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                    {
                        long micros = getDataStream(encoded).readLong();
                        long secs = micros / 1000000;
                        lvalue = seconds2000NoTZ(secs);
                        nanos = (int)(micros - secs * 1000000) * 1000;
                        lvalueType = Types.TIMESTAMP;
                    }
                    break;
                case DAYS_2000:
                    lvalue = days2000(getDataStream(encoded).readInt());
                    lvalueType = Types.DATE;
                    break;
                case TIME_FLOAT64_SECS_NOTZ:
                    lvalue = timeSecsNoTZ((int)getDataStream(encoded).readDouble());
                    lvalueType = Types.TIME;
                    break;
                case TIME_INT64_MICROS_NOTZ:
                    lvalue = timeSecsNoTZ((int)(getDataStream(encoded).readLong() / 1000000L));
                    lvalueType = Types.TIME;
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
                throw new UnsupportedCharsetException(encoding);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("IO error reading from byte array", ex);
            }
        }
        if (lvalueType != Types.NULL) {
            // lvalue will self-identify as a BIGINT, so valueFromObject
            // is only okay for integer types.
            int targetJDBCType = typesTranslator.jdbcType(targetType);
            if (lvalueType == Types.BIGINT) {
                boolean isIntegerType;
                switch (targetJDBCType) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    isIntegerType = true;
                    break;
                default:
                    isIntegerType = false;
                }
                if (isIntegerType) {
                    // Can still optimize this case.
                    Value source = new Value(targetType);
                    typesTranslator.setIntegerValue(source, lvalue);
                    bindings.setValue(index, source);
                    return;
                }
                // Fall through to valueFromObject with Long.
                value = lvalue;
            }
            else if (lvalueType == targetJDBCType) {
                // Matches; can set directly.
                Value source = new Value(targetType);
                typesTranslator.setTimestampMillisValue(source, lvalue, nanos);
                bindings.setValue(index, source);
                return;
            }
            else {
                // Otherwise need to make sure fromObject is given
                // something tagged as date-like.
                // TODO: When Postgres takes parameter type into
                // account in optimization, might not be as necessary.
                Value source = new Value(typesTranslator.typeClassForJDBCType(lvalueType)
                                         .instance(true));
                typesTranslator.setTimestampMillisValue(source, lvalue, nanos);
                Value target = new Value(targetType);
                TExecutionContext context =
                    new TExecutionContext(Collections.singletonList(source.getType()),
                                          targetType,
                                          queryContext);
                TInstance.tClass(targetType).fromObject(context, source, target);
                bindings.setValue(index, target);
                return;
            }
        }

        ValueSource source = ValueSources.valuefromObject(value, targetType,
                                                          queryContext);
        bindings.setValue(index, source);
    }

    private static DataInputStream getDataStream(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static long seconds2000NoTZ(long secs) {
        long millis = (secs + 946684800) * 1000; // 2000-01-01 00:00:00-UTC.
        DateTimeZone dtz = DateTimeZone.getDefault();
        millis -= dtz.getOffset(millis);
        return millis;
    }

    private static long days2000(int days) {
        return seconds2000NoTZ(days * 86400);
    }

    private static long timeSecsNoTZ(int secs) {
        int h = secs / 3600;
        int m = (secs / 60) % 60;
        int s = secs % 60;
        DateTime dt = new DateTime(1970, 1, 1, h, m, s);
        return dt.getMillis();
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
