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
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

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
                            QueryContext queryContext, TypesRegistryService typesRegistryService) {
       
        TInstance targetType = type != null ? type.getType() : null;
        // TODO: is this correct, try to write a test that executes this path, probably `SELECT ?`
        if (targetType == null)
            targetType = typesTranslator.typeForString();
        long lvalue = 0;
        int nanos = 0;
        int lvalueType = Types.NULL;
        ValueSource source;
        if (encoded == null) {
            source = new Value(null);
        }
        else if (!binary) {
            try {
                source = new Value(MString.varchar(), new String(encoded, encoding));
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException(encoding);
            }
        }
        else {
            try {
                switch (type.getBinaryEncoding()) {
                case BINARY_OCTAL_TEXT:
                    source = new Value(MBinary.VARBINARY.instance(false), encoded);
                    break;
                case INT_8:
                case INT_16:
                case INT_32:
                case INT_64: // Types.BIGINT
                    // Go by the length sent rather than the implied type.
                    source = decodeIntegerType(encoded);
                    break;
                case FLOAT_32:

                    source = new Value(MApproximateNumber.FLOAT.instance(false), getDataStream(encoded).readFloat());
                    break;
                case FLOAT_64:
                    source = new Value(MApproximateNumber.DOUBLE.instance(false), getDataStream(encoded).readDouble());
                    break;
                case STRING_BYTES:
                    source = decodeString(encoded);
                    break;
                case BOOLEAN_C:
                    source = decodeBoolean(encoded);
                    break;
                case TIMESTAMP_FLOAT64_SECS_2000_NOTZ: // Types.TIMESTAMP
                    source = decodeTimestampFloat64Secs2000NoTZ(encoded);
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ: // Types.TIMESTAMP
                    source = decodeTimestampInt64Micros2000NoTZ(encoded);
                    break;
                case DAYS_2000: // DATE
                    source = decodeDays2000(encoded);
                    break;
                case TIME_FLOAT64_SECS_NOTZ: // TIME
                    source = decodeTimeFloat64SecsNoTZ(encoded);
                    break;
                case TIME_INT64_MICROS_NOTZ: // TIME
                    source = decodeTimeInt64MicrosNoTZ(encoded);
                    break;
                case DECIMAL_PG_NUMERIC_VAR:
                    source = decodeDecimalPgNumericVar(encoded);
                    break;
                // TODO GUID
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
        TCast cast = typesRegistryService.getCastsResolver().cast(source.getType(), targetType);
        TExecutionContext context =
                new TExecutionContext(Collections.singletonList(source.getType()),
                        targetType,
                        queryContext);
        Value target = new Value(targetType);
        cast.evaluate(context, source, target);
        bindings.setValue(index, target);
    }

    private ValueSource decodeTimeInt64MicrosNoTZ(byte[] encoded) {
        // lvalue = timeSecsNoTZ((int)(getDataStream(encoded).readLong() / 1000000L));
        // lvalueType = Types.TIME;

        assert false : "handle decodeTimeInt64MicrosNoTZ";
        return null;
    }

    private ValueSource decodeTimeFloat64SecsNoTZ(byte[] encoded) {
        // lvalue = timeSecsNoTZ((int)getDataStream(encoded).readDouble());
        // lvalueType = Types.TIME;
        assert false : "handle decodeTimeFloat64SecsNoTZ";
        return null;
    }

    private ValueSource decodeDays2000(byte[] encoded) {
        // lvalue = days2000(getDataStream(encoded).readInt());

        assert false : "handle decodeDays2000";
        return null;
    }

    private ValueSource decodeBoolean(byte[] encoded) {
        // getDataStream(encoded).readDouble();

        assert false : "handle decodeBoolean";
        return null;
    }

    private ValueSource decodeString(byte[] encoded) {
        // new String(encoded, encoding)

        assert false : "handle decodeString";
        return null;
    }

    private ValueSource decodeDecimalPgNumericVar(byte[] encoded) {
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

    private ValueSource decodeTimestampInt64Micros2000NoTZ(byte[] encoded) {
//
//        long micros = getDataStream(encoded).readLong();
//        long secs = micros / 1000000;
//        lvalue = seconds2000NoTZ(secs);
//        nanos = (int)(micros - secs * 1000000) * 1000;
//        lvalueType = Types.TIMESTAMP;

        assert false : "handle decodeTimestampInt64Micros2000NoTZ";
        return null;
    }

    private ValueSource decodeTimestampFloat64Secs2000NoTZ(byte[] encoded) {
//
//            double dsecs = getDataStream(encoded).readDouble();
//            long secs = (long)dsecs;
//            lvalue = seconds2000NoTZ(secs);
//            nanos = (int)((dsecs - secs) * 1.0e9);
//            lvalueType = Types.TIMESTAMP;

        assert false : "handle decodeTimestampFloat64Secs2000NoTZ";
        return null;
    }

    public ValueSource decodeIntegerType(byte[] encoded) throws IOException {
        // TODO unsigned?
        switch (encoded.length) {
        case 1:
            return new Value(MNumeric.TINYINT.instance(false), getDataStream(encoded).read());
        case 2:
            return new Value(MNumeric.SMALLINT.instance(false), getDataStream(encoded).readShort());
        case 4:
            return new Value(MNumeric.INT.instance(false), getDataStream(encoded).readInt());
        case 8:
            return new Value(MNumeric.BIGINT.instance(false), getDataStream(encoded).readLong());
        default:
            throw new AkibanInternalException("Not an integer size: " + encoded);
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
