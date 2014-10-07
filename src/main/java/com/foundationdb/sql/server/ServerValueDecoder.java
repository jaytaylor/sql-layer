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
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.UnderlyingType;
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
        long lvalue = 0;
        int nanos = 0;
        int lvalueType = Types.NULL;
        ValueSource source;
        if (encoded == null) {
            Value value = new Value(targetType);
            value.putNull();
            source = value;
        }
        else if (!binary) {
            try {
                new String(encoded, encoding);
                source = new Value(targetType);
                assert false : "TODO non-binary transfer";
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException(encoding);
            }
        }
        else {
            try {
                switch (type.getBinaryEncoding()) {
                case BINARY_OCTAL_TEXT:
                    source = decodeBinaryOctalText(encoded, targetType);
                    break;
                case INT_8:
                case INT_16:
                case INT_32:
                case INT_64: // Types.BIGINT
                    // Go by the length sent rather than the implied type.
                    source = decodeIntegerType(encoded, targetType);
                    break;
                case FLOAT_32:
                    source = decodeFloat(encoded, targetType);
                    break;
                case FLOAT_64:
                    source = decodeDouble(encoded, targetType);
                    break;
                case STRING_BYTES:
                    source = decodeString(encoded, targetType);
                    break;
                case BOOLEAN_C:
                    source = decodeBoolean(encoded, targetType);
                    break;
                case TIMESTAMP_FLOAT64_SECS_2000_NOTZ: // Types.TIMESTAMP
                    source = decodeTimestampFloat64Secs2000NoTZ(encoded, targetType);
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ: // Types.TIMESTAMP
                    source = decodeTimestampInt64Micros2000NoTZ(encoded, targetType);
                    break;
                case DAYS_2000: // DATE
                    source = decodeDays2000(encoded, targetType);
                    break;
                case TIME_FLOAT64_SECS_NOTZ: // TIME
                    source = decodeTimeFloat64SecsNoTZ(encoded, targetType);
                    break;
                case TIME_INT64_MICROS_NOTZ: // TIME
                    source = decodeTimeInt64MicrosNoTZ(encoded, targetType);
                    break;
                case DECIMAL_PG_NUMERIC_VAR:
                    source = decodeDecimalPgNumericVar(encoded, targetType);
                    break;
                default:
                    source = new Value(targetType);
                    assert false : "TODO default case";
                    return;
//                    if (targetType.typeClass() instanceof TString)
//                        value = new String(encoded, encoding);
//                    else
//                        value = encoded;
//                    break;
                }
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException(encoding);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("IO error reading from byte array", ex);
            }
        }
//        if (lvalueType != Types.NULL) {
//            // lvalue will self-identify as a BIGINT, so valueFromObject
//            // is only okay for integer types.
//            int targetJDBCType = typesTranslator.jdbcType(targetType);
//            if (lvalueType == Types.BIGINT) {
//                boolean isIntegerType;
//                switch (targetJDBCType) {
//                case Types.TINYINT:
//                case Types.SMALLINT:
//                case Types.INTEGER:
//                case Types.BIGINT:
//                    isIntegerType = true;
//                    break;
//                default:
//                    isIntegerType = false;
//                }
//                if (isIntegerType) {
//                    // Can still optimize this case.
//                    Value source = new Value(targetType);
//                    typesTranslator.setIntegerValue(source, lvalue);
//                    bindings.setValue(index, source);
//                    return;
//                }
//                // Fall through to valueFromObject with Long.
//                value = lvalue;
//            }
//            else if (lvalueType == targetJDBCType) {
//                // Matches; can set directly.
//                Value source = new Value(targetType);
//                typesTranslator.setTimestampMillisValue(source, lvalue, nanos);
//                bindings.setValue(index, source);
//                return;
//            }
//            else {
//                // Otherwise need to make sure fromObject is given
//                // something tagged as date-like.
//                // TODO: When Postgres takes parameter type into
//                // account in optimization, might not be as necessary.
//                Value source = new Value(typesTranslator.typeClassForJDBCType(lvalueType)
//                                         .instance(true));
//                typesTranslator.setTimestampMillisValue(source, lvalue, nanos);
//                Value target = new Value(targetType);
//                TExecutionContext context =
//                    new TExecutionContext(Collections.singletonList(source.getType()),
//                                          targetType,
//                                          queryContext);
//                TInstance.tClass(targetType).fromObject(context, source, target);
//                bindings.setValue(index, target);
//                return;
//            }
//        }
//        switch (TInstance.underlyingType(targetType)) {
//            case STRING:
//                if (!(value instanceof String)) {
//                    throw new InvalidParameterValueException(value.toString());
//                }
//        }
//
//        source = ValueSources.valuefromObject(value, targetType,
//                queryContext);

        bindings.setValue(index, source);
    }

    private ValueSource decodeBinaryOctalText(byte[] encoded, TInstance targetType) {
        // if (targetType.typeClass() instanceof TString)
        //     value = new String(encoded, encoding);
        // else
        //     value = encoded;
        assert false : "handle decodeBinaryOctalText";
        return null;
    }

    private ValueSource decodeTimeInt64MicrosNoTZ(byte[] encoded, TInstance targetType) {
        // lvalue = timeSecsNoTZ((int)(getDataStream(encoded).readLong() / 1000000L));
        // lvalueType = Types.TIME;

        assert false : "handle decodeTimeInt64MicrosNoTZ";
        return null;
    }

    private ValueSource decodeTimeFloat64SecsNoTZ(byte[] encoded, TInstance targetType) {
        // lvalue = timeSecsNoTZ((int)getDataStream(encoded).readDouble());
        // lvalueType = Types.TIME;
        assert false : "handle decodeTimeFloat64SecsNoTZ";
        return null;
    }

    private ValueSource decodeDays2000(byte[] encoded, TInstance targetType) {
        // lvalue = days2000(getDataStream(encoded).readInt());

        assert false : "handle decodeDays2000";
        return null;
    }

    private ValueSource decodeBoolean(byte[] encoded, TInstance targetType) {
        // getDataStream(encoded).readDouble();

        assert false : "handle decodeBoolean";
        return null;
    }

    private ValueSource decodeDouble(byte[] encoded, TInstance targetType) {
        // getDataStream(encoded).readDouble();

        assert false : "handle decodeDouble";
        return null;
    }

    private ValueSource decodeString(byte[] encoded, TInstance targetType) {
        // new String(encoded, encoding)

        assert false : "handle decodeString";
        return null;
    }

    private ValueSource decodeFloat(byte[] encoded, TInstance targetType) {
        // getDataStream(encoded).readFloat();

        assert false : "handle decodeFloat";
        return null;
    }

    private ValueSource decodeDecimalPgNumericVar(byte[] encoded, TInstance targetType) {
//
//        DataInputStream dstr = getDataStream(encoded);
//        short ndigits = dstr.readShort();
//        short[] digits = new short[ndigits + 4];
//        digits[0] = ndigits;
//        for (int i = 1; i < digits.length; i++) {
//            digits[i] = dstr.readShort();
//        }
//        value = pgNumericVar(digits);
        assert false : "handle decodeDecimalPgNumericVar";
        return null;
    }

    private ValueSource decodeTimestampInt64Micros2000NoTZ(byte[] encoded, TInstance targetType) {
//
//        long micros = getDataStream(encoded).readLong();
//        long secs = micros / 1000000;
//        lvalue = seconds2000NoTZ(secs);
//        nanos = (int)(micros - secs * 1000000) * 1000;
//        lvalueType = Types.TIMESTAMP;

        assert false : "handle decodeTimestampInt64Micros2000NoTZ";
        return null;
    }

    private ValueSource decodeTimestampFloat64Secs2000NoTZ(byte[] encoded, TInstance targetType) {
//
//            double dsecs = getDataStream(encoded).readDouble();
//            long secs = (long)dsecs;
//            lvalue = seconds2000NoTZ(secs);
//            nanos = (int)((dsecs - secs) * 1.0e9);
//            lvalueType = Types.TIMESTAMP;

        assert false : "handle decodeTimestampFloat64Secs2000NoTZ";
        return null;
    }

    public ValueSource decodeIntegerType(byte[] encoded, TInstance targetType) throws IOException {
        long lvalue;
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
        UnderlyingType underlyingType = TInstance.underlyingType(targetType);

        Value source = new Value(targetType);
        if (underlyingType == null) {
            assert false : "handle null; value must be null if type is null";
        }
        switch (underlyingType) {
            case BOOL:
                source.putBool(lvalue != 0);
                return source;
            case INT_8:
            case INT_16:
            case UINT_16:
            case INT_32:
            case INT_64:
                typesTranslator.setIntegerValue(source, lvalue);
                return source;
            case FLOAT:
                assert false : "implement int to float";
            case DOUBLE:
                assert false : "implement int to double";
            case BYTES:
                source.putBytes(encoded);
                return source;
            case STRING:
                // TODO: verify that this is ok to be null. valueFromObject used null if the underlyingType was string
                // and StringFactory.NULL_COLLATION_ID if the underlyingType was null
                source.putString(Long.toString(lvalue), null);
                return source;
            default:
                throw new UnknownDataTypeException(underlyingType.toString());
        }
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
