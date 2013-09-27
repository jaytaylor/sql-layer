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
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;

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
    public void decodePValue(byte[] encoded, ServerType type, boolean binary,
                             QueryBindings bindings, int index) {
       
        TInstance decodedType = null;
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
                        decodedType = MNumeric.TINYINT.instance(true);
                        break;
                    case 2:
                        value = (long)getDataStream(encoded).readShort();
                        decodedType = MNumeric.MEDIUMINT.instance(true);
                        break;
                    case 4:
                        value = (long)getDataStream(encoded).readInt();
                        decodedType = MNumeric.INT.instance(true);
                        break;
                    case 8:
                        value = getDataStream(encoded).readLong();
                        decodedType = MNumeric.BIGINT.instance(true);
                        break;
                    default:
                        throw new AkibanInternalException("Not an integer size: " + encoded);
                    }
                    break;
                case FLOAT_32:
                    value = getDataStream(encoded).readFloat();
                    decodedType = MApproximateNumber.FLOAT.instance(true);
                    break;
                case FLOAT_64:
                    value = getDataStream(encoded).readDouble();
                    decodedType = MApproximateNumber.DOUBLE.instance(true);
                    break;
                case STRING_BYTES:
                    value = new String(encoded, encoding);
                    decodedType = MString.varcharFor((String)value);
                    break;
                case BOOLEAN_C:
                    value = (encoded[0] != 0);
                    break;
                case TIMESTAMP_FLOAT64_SECS_2000_NOTZ:
                    value = seconds2000NoTZ((long)getDataStream(encoded).readDouble());
                    decodedType = MDatetimes.TIMESTAMP.instance(true);
                    break;
                case TIMESTAMP_INT64_MICROS_2000_NOTZ:
                    value = seconds2000NoTZ(getDataStream(encoded).readLong() / 1000000L);
                    decodedType = MDatetimes.TIMESTAMP.instance(true);
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
        PValueSource source = PValueSources.pValuefromObject(value, targetType);
        bindings.setPValue(index, source);
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
