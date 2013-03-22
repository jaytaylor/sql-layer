
package com.akiban.sql.server;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

import org.joda.time.DateTimeZone;
import java.math.BigDecimal;
import java.io.*;

/** Decode values from external representation into query bindings. */
public class ServerValueDecoder
{
    private String encoding;
    private FromObjectValueSource objectSource;

    public ServerValueDecoder(String encoding) {
        this.encoding = encoding;
        objectSource = new FromObjectValueSource();
    }

    /** Decode the given value into a the given bindings at the given position.
     */
    public  <T extends ServerSession> void decodeValue(byte[] encoded, ServerType type, boolean binary,
                                                       ServerQueryContext<T> context, int index) {
        AkType targetType = null;
        if (type != null)
            targetType = type.getAkType();
        if (targetType == null)
            targetType = AkType.VARCHAR;
        Object value;
        AkType decodedType = null; // If not evident from reflection.
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
                    value = encoded;
                    break;
                case INT_8:
                    value = (long)getDataStream(encoded).read();
                    decodedType = AkType.INT;
                    break;
                case INT_16:
                    value = (long)getDataStream(encoded).readShort();
                    decodedType = AkType.INT;
                    break;
                case INT_32:
                    value = (long)getDataStream(encoded).readInt();
                    decodedType = AkType.INT;
                    break;
                case INT_64:
                    value = getDataStream(encoded).readLong();
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
                    value = seconds2000NoTZ((long)getDataStream(encoded).readDouble());
                    decodedType = AkType.TIMESTAMP;
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                    value = seconds2000NoTZ(getDataStream(encoded).readLong() / 1000000L);
                    decodedType = AkType.TIMESTAMP;
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
        if (decodedType != null)
            objectSource.setExplicitly(value, decodedType);
        else
            objectSource.setReflectively(value);
        context.setValue(index, objectSource, targetType);
    }
   
    public <T extends ServerSession> void decodePValue(byte[] encoded, ServerType type, boolean binary,
                                                       ServerQueryContext<T> context, int index) {
        AkType targetType = null;
        if (type != null)
            targetType = type.getAkType();
        if (targetType == null)
            targetType = AkType.VARCHAR;
        Object value;
        AkType decodedType = null; // If not evident from reflection.
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
                    value = encoded;
                    break;
                case INT_8:
                    value = (long)getDataStream(encoded).read();
                    decodedType = AkType.INT;
                    break;
                case INT_16:
                    value = (long)getDataStream(encoded).readShort();
                    decodedType = AkType.INT;
                    break;
                case INT_32:
                    value = (long)getDataStream(encoded).readInt();
                    decodedType = AkType.INT;
                    break;
                case INT_64:
                    value = getDataStream(encoded).readLong();
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
                    value = seconds2000NoTZ((long)getDataStream(encoded).readDouble());
                    decodedType = AkType.TIMESTAMP;
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                    value = seconds2000NoTZ(getDataStream(encoded).readLong() / 1000000L);
                    decodedType = AkType.TIMESTAMP;
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
        if (decodedType == null)
            decodedType = targetType;
        PValueSource source = PValueSources.fromObject(value, decodedType).value();
        context.setPValue(index, source);
    }

    private static DataInputStream getDataStream(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static long seconds2000NoTZ(long secs) {
        long unixtime = secs + 946702800L; // 2000-01-01 00:00:00-UTC.
        DateTimeZone dtz = DateTimeZone.getDefault();
        unixtime -= (dtz.getOffset(unixtime * 1000) - dtz.getStandardOffset(unixtime * 1000)) / 1000;
        return unixtime;
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
