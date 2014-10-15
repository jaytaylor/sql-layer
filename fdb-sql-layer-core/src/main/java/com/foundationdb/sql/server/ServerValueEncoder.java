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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.error.ZeroDateTimeException;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;

import com.foundationdb.util.Strings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collections;
import java.util.Date;
import java.io.*;

/** Encode result values for transmission. */
public class ServerValueEncoder
{
    
    public static enum ZeroDateTimeBehavior {
        NONE(null),
        EXCEPTION("exception"),
        ROUND("round"),
        CONVERT_TO_NULL("convertToNull");

        private String propertyName;

        ZeroDateTimeBehavior(String propertyName) {
            this.propertyName = propertyName;
        }
        
        public static ZeroDateTimeBehavior fromProperty(String name) {
            if (name == null) return NONE;
            for (ZeroDateTimeBehavior zdtb : values()) {
                if (name.equals(zdtb.propertyName))
                    return zdtb;
            }
            throw new InvalidParameterValueException(String.format("Invalid name: %s for ZeroDateTimeBehavior", name));
        }
    }

  
    public static final ValueSource ROUND_ZERO_DATETIME_SOURCE = new Value(MDateAndTime.DATETIME.instance(false),
                                                                           MDateAndTime.encodeDateTime(1, 1, 1, 0, 0, 0));
    public static final ValueSource ROUND_ZERO_DATE_SOURCE = new Value(MDateAndTime.DATE.instance(false),
                                                                       MDateAndTime.encodeDate(1, 1, 1));
    
    private final TypesTranslator typesTranslator;
    private final String encoding;
    private ZeroDateTimeBehavior zeroDateTimeBehavior;
    private FormatOptions options;
    private final ByteArrayOutputStream byteStream;
    private final PrintWriter printWriter;
    private final AkibanAppender appender;
    private DataOutputStream dataStream;

    public ServerValueEncoder(TypesTranslator typesTranslator, String encoding, FormatOptions options) {
        this(typesTranslator, encoding, new ByteArrayOutputStream(), options);
    }

    public ServerValueEncoder(TypesTranslator typesTranslator, String encoding, 
                              ZeroDateTimeBehavior zeroDateTimeBehavior, FormatOptions options) {
        this(typesTranslator, encoding, options);
        this.zeroDateTimeBehavior = zeroDateTimeBehavior;
    }

    public ServerValueEncoder(TypesTranslator typesTranslator, String encoding, ByteArrayOutputStream byteStream, 
                              FormatOptions options) {
        this.typesTranslator = typesTranslator;
        this.encoding = encoding;
        this.byteStream = byteStream;
        this.options = options;
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(byteStream, encoding));
        }
        catch (UnsupportedEncodingException ex) {
            throw new UnsupportedCharsetException(encoding);
        }
        // If the target encoding is UTF-8, we can support
        // canAppendBytes() for properly encoded source strings.
        if ("UTF-8".equals(encoding))
            appender = AkibanAppender.of(byteStream, printWriter, "UTF-8");
        else
            appender = AkibanAppender.of(printWriter);
    }

    public String getEncoding() {
        return encoding;
    }

    public ByteArrayOutputStream getByteStream() {
        printWriter.flush();
        return byteStream;
    }

    public AkibanAppender getAppender() {
        return appender;
    }

    public DataOutputStream getDataStream() {
        printWriter.flush();
        if (dataStream == null)
            dataStream = new DataOutputStream(byteStream);
        return dataStream;
    }

  
    /**
     * Encode the given value into a stream that can then be passed
     * to
     * <code>writeByteStream</code>.
     */
    public ByteArrayOutputStream encodeValue(ValueSource value, ServerType type,
                                             boolean binary) throws IOException {
        if (value.isNull())
            return null;
        if ((zeroDateTimeBehavior != ZeroDateTimeBehavior.NONE) &&
            (((type.getType().typeClass() == MDateAndTime.DATE) &&
              (value.getInt32() == 0)) ||
             ((type.getType().typeClass() == MDateAndTime.DATETIME) &&
              (value.getInt64() == 0)))) {
            switch (zeroDateTimeBehavior) {
            case EXCEPTION:
                throw new ZeroDateTimeException();
            case ROUND:
                value = (type.getType().typeClass() == MDateAndTime.DATETIME)
                        ? ROUND_ZERO_DATETIME_SOURCE
                        : ROUND_ZERO_DATE_SOURCE;
                break;
            case CONVERT_TO_NULL:
                return null;
            }
        }
        reset();
        appendValue(value, type, binary);
        return getByteStream();
    }

    /** Encode the given direct value. */
    public ByteArrayOutputStream encodePObject(Object value, ServerType type, 
                                               boolean binary) throws IOException {
        if (value == null)
            return null;
        reset();
        appendPObject(value, type, binary);
        return getByteStream();
    }

    /** Reset the contents of the buffer. */
    public void reset() {
        getByteStream().reset();
    }
    
    /** Append the given value to the buffer. */
    public void appendValue(ValueSource value, ServerType type, boolean binary)
            throws IOException {
        if (!binary) {
            // Handle unusual text encoding of binary types.
            switch (type.getBinaryEncoding()) {
            case BINARY_OCTAL_TEXT:
                processBinaryText(value);
                break;

            default:
                type.getType().format(value, appender);
                break;
            }
        }
        else {
            switch (type.getBinaryEncoding()) {
            case BINARY_OCTAL_TEXT:
                getByteStream().write(value.getBytes());
                break;
            case INT_8:
                getDataStream().write((byte)typesTranslator.getIntegerValue(value));
                break;
            case INT_16:
                getDataStream().writeShort((short)typesTranslator.getIntegerValue(value));
                break;
            case INT_32:
                getDataStream().writeInt((int)typesTranslator.getIntegerValue(value));
                break;
            case INT_64:
                getDataStream().writeLong(typesTranslator.getIntegerValue(value));
                break;
            case FLOAT_32:
                getDataStream().writeFloat(value.getFloat());
                break;
            case FLOAT_64:
                getDataStream().writeDouble(value.getDouble());
                break;
            case STRING_BYTES:
                getByteStream().write(value.getString().getBytes(encoding));
                break;
            case BOOLEAN_C:
                getDataStream().write(value.getBoolean() ? 1 : 0);
                break;
            case TIMESTAMP_FLOAT64_SECS_2000_NOTZ:
                getDataStream().writeDouble(seconds2000NoTZ(typesTranslator.getTimestampMillisValue(value)) +
                                            typesTranslator.getTimestampNanosValue(value) / 1.0e9);
                break;
            case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                getDataStream().writeLong(seconds2000NoTZ(typesTranslator.getTimestampMillisValue(value)) * 1000000L +
                                          typesTranslator.getTimestampNanosValue(value) / 1000);
                break;
            case DAYS_2000:
                getDataStream().writeInt(days2000(typesTranslator.getTimestampMillisValue(value)));
                break;
            case TIME_FLOAT64_SECS_NOTZ:
                getDataStream().writeDouble(timeSecsNoTZ(typesTranslator.getTimestampMillisValue(value)));
                break;
            case TIME_INT64_MICROS_NOTZ:
                getDataStream().writeLong(timeSecsNoTZ(typesTranslator.getTimestampMillisValue(value)) * 1000000L);
                break;
            case DECIMAL_PG_NUMERIC_VAR:
                for (short d : pgNumericVar(typesTranslator.getDecimalValue(value))) {
                    getDataStream().writeShort(d);
                }
                break;
            case NONE:
            default:
                throw new UnsupportedOperationException("No binary encoding for " + type);
            }
        }
    }   

    private void processBinaryText(ValueSource value) {
        FormatOptions.BinaryFormatOption bfo = options.get(FormatOptions.BinaryFormatOption.class);
        String formattedString = bfo.format(value.getBytes());
        printWriter.append(formattedString);
    }
    
    /** Append the given direct object to the buffer. */
    public void appendPObject(Object value, ServerType type, boolean binary) 
            throws IOException {
        if (type.getType().typeClass() instanceof TString && value instanceof String)
        {
            // Optimize the common case of directly encoding a string.
            printWriter.write((String)value);
            return;
        }

        ValueSource source = valuefromObject(value, type);
        appendValue(source, type, binary);
    }

    public ValueSource valuefromObject(Object value, ServerType type) {
        if (value instanceof Date) {
            TInstance dateType = javaDateTInstance(value);
            Value dateValue = new Value(dateType);
            typesTranslator.setTimestampMillisValue(dateValue, ((Date)value).getTime(),
                                                    (value instanceof java.sql.Timestamp) ?
                                                    ((java.sql.Timestamp)value).getNanos() : 0);
            TInstance targetType = type.getType();
            if (dateType.equals(targetType))
                return dateValue;
            TExecutionContext context =
                new TExecutionContext(Collections.singletonList(dateType),
                                      targetType, null);
            Value result = new Value(targetType);
            targetType.typeClass().fromObject(context, dateValue, result);
            return result;
        }
        else {
            // TODO this is inefficient, but I want to get it working.
            return ValueSources.valuefromObject(value, type.getType());
        }
    }
    
    private TInstance javaDateTInstance(Object value) {
        int jdbcType;
        if (value instanceof java.sql.Date) {
            jdbcType = Types.DATE;
        } else if (value instanceof java.sql.Time) {
            jdbcType = Types.TIME;
        } else {
            jdbcType = Types.TIMESTAMP;
        }
        return typesTranslator.typeClassForJDBCType(jdbcType).instance(true);
    }

    public void appendString(String string) throws IOException {
        printWriter.write(string);
    }

    public PrintWriter getWriter() {
        return printWriter;
    }

    /** Adjust milliseconds since 1970-01-01 00:00:00-UTC to seconds since
     * 2000-01-01 00:00:00 timezoneless. A conversion from local time
     * to UTC involves an offset that varies for Summer time. A
     * conversion from local time to timezoneless just removes the
     * zone as though all days were the same length.
     */
    private static long seconds2000NoTZ(long millis) {
        DateTimeZone dtz = DateTimeZone.getDefault();
        millis += dtz.getOffset(millis);
        return millis / 1000 - 946684800; // 2000-01-01 00:00:00-UTC.
    }

    public static int days2000(long millis) {
        long secs = seconds2000NoTZ(millis);
        return (int)(secs / 86400);
    }

    public static int timeSecsNoTZ(long millis) {
        DateTime dt = new DateTime(millis);
        return dt.getSecondOfDay();
    }

    private static final short NUMERIC_POS = 0x0000;
    private static final short NUMERIC_NEG = 0x4000;
    private static final short NUMERIC_NAN = (short)0xC000;

    private static short[] pgNumericVar(BigDecimal n) {
        short ndigits, weight, sign, dscale;
        dscale = (short)n.scale();
        if (dscale < 0) dscale = 0;
        String s = n.toPlainString();
        int lpos = 0;
        sign = NUMERIC_POS;
        if (s.charAt(lpos) == '-') {
            sign = NUMERIC_NEG;
            lpos++;
        }
        int dposl = s.indexOf('.', lpos), dposr;
        if (dposl < 0) 
            dposr = dposl = s.length();
        else
            dposr = dposl + 1;
        int nleft = (dposl - lpos + 3) / 4;
        weight = (short)(nleft - 1);
        int nright = (s.length() - dposr + 3) / 4;
        ndigits = (short)(nleft + nright);
        while ((ndigits > 0) && (pgNumericDigit(s, ndigits-1, 
                                                lpos, dposl, dposr, 
                                                nleft, nright) == 0)) {
            ndigits--;
        }
        short[] digits = new short[ndigits+4];
        digits[0] = ndigits;
        digits[1] = weight;
        digits[2] = sign;
        digits[3] = dscale;
        for (int i = 0; i < ndigits; i++) {
            digits[i + 4] = pgNumericDigit(s, i, lpos, dposl, dposr, nleft, nright);
        }
        return digits;
    }

    private static short pgNumericDigit(String s, int index, 
                                        int lpos, int dposl, int dposr, 
                                        int nleft, int nright) {
        short result = 0;
        if (index < nleft) {
            int pos = dposl + (index - nleft) * 4;
            for (int i = 0; i < 4; i++) {
                result = (short)(result * 10);
                if (pos >= lpos)
                    result += s.charAt(pos) - '0';
                pos++;
            }
        }
        else {
            int pos = dposr + (index - nleft) * 4;
            for (int i = 0; i < 4; i++) {
                result = (short)(result * 10);
                if (pos < s.length())
                    result += s.charAt(pos) - '0';
                pos++;
            }
        }
        return result;
    }

}
