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
import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
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
        if (targetType == null && encoded != null) {
            throw new UnknownDataTypeException(null);
        }
        ValueSource source;
        if (encoded == null) {
            Value value = new Value(targetType);
            value.putNull();
            bindings.setValue(index, value);
            return;
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
                case BOOLEAN_C:
                    source = new Value(AkBool.INSTANCE.instance(false), encoded[0] != 0);
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ: // Types.TIMESTAMP
                    source = decodeTimestampInt64Micros2000NoTZ(encoded);
                    break;
                case UUID:
                    Value value = new Value(AkGUID.INSTANCE.instance(false));
                    value.putObject(AkGUID.bytesToUUID(encoded, 0));
                    source = value;
                    break;
                // Note: these types had previous implementations, but I couldn't exercise them in tests to verify
                // either with jdbc or pg8000. If you run into them, try looking at the log for this file, it most
                // likely has a correct starting point
                case STRING_BYTES:
                case TIMESTAMP_FLOAT64_SECS_2000_NOTZ: // Types.TIMESTAMP
                case DAYS_2000: // DATE
                case TIME_FLOAT64_SECS_NOTZ: // TIME
                case TIME_INT64_MICROS_NOTZ: // TIME
                case DECIMAL_PG_NUMERIC_VAR:
                default:
                    throw new UnknownDataTypeException(type.toString());
                }
            }
            catch (UnsupportedEncodingException ex) {
                throw new UnsupportedCharsetException(encoding);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("IO error reading from byte array", ex);
            }
        }
        TCast cast = typesRegistryService.getCastsResolver().cast(source.getType(), targetType);
        TExecutionContext context =
                new TExecutionContext(Collections.singletonList(source.getType()),
                        targetType,
                        queryContext);
        Value target = new Value(targetType);
        cast.evaluate(context, source, target);
        bindings.setValue(index, target);
    }

    private ValueSource decodeTimestampInt64Micros2000NoTZ(byte[] encoded) throws IOException {
        long micros = getDataStream(encoded).readLong();
        long secs = micros / 1000000;
        long milliseconds = seconds2000NoTZ(secs);
        int nanos = (int) (micros - secs * 1000000) * 1000;
        Value source = new Value(MDateAndTime.TIMESTAMP.instance(false));
        typesTranslator.setTimestampMillisValue(source, milliseconds, nanos);
        return source;
    }

    public ValueSource decodeIntegerType(byte[] encoded) throws IOException {
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

}
