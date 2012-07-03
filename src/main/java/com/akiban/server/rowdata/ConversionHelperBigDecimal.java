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

package com.akiban.server.rowdata;

import com.akiban.server.AkServerUtil;
import com.akiban.server.types.ValueSourceException;
import com.akiban.util.AkibanAppender;

import java.math.BigDecimal;

public final class ConversionHelperBigDecimal {

    // "public" methods (though still only available within-package)

    /**
     * Decodes the field from the given RowData into the given StringBuilder.
     * @param fieldDef the fieldDef whose type is a decimal
     * @param from the underlying byte array
     * @param locationAndOffset the byte's location and offset, packed as RowData packs them
     * @param appender the appender to use
     * @throws NullPointerException if any arguments are null
     * @throws ValueSourceException if the string can't be parsed to a BigDecimal
     */
    public static void decodeToString(FieldDef fieldDef, byte[] from, long locationAndOffset, AkibanAppender appender) {
        final int precision = fieldDef.getTypeParameter1().intValue();
        final int scale = fieldDef.getTypeParameter2().intValue();
        final int location = (int) locationAndOffset;

        try {
            decodeToString(from, location, precision, scale, appender);
        } catch (NumberFormatException e) {
            StringBuilder errSb = new StringBuilder();
            errSb.append("in field[");
            errSb.append(fieldDef.getRowDef().getRowDefId()).append('.').append(fieldDef.getFieldIndex());
            errSb.append(" decimal(");
            errSb.append(fieldDef.getTypeParameter1()).append(',').append(fieldDef.getTypeParameter2());
            errSb.append(")] 0x");
            final int bytesLen = (int) (locationAndOffset >>> 32);
            AkServerUtil.hex(AkibanAppender.of(errSb), from, location, bytesLen);
            errSb.append(": ").append( e.getMessage() );
            throw new ValueSourceException(errSb.toString(), e);
        }
    }


    public static int fromObject(FieldDef fieldDef, BigDecimal value, byte[] dest, int offset) {
        final int declPrec = fieldDef.getTypeParameter1().intValue();
        final int declScale = fieldDef.getTypeParameter2().intValue();

        return fromObject(value, dest, offset, declPrec, declScale);
    }

    public static byte[] bytesFromObject(BigDecimal value, int declPrec, int declScale) {
        final int declIntSize = calcBinSize(declPrec - declScale);
        final int declFracSize = calcBinSize(declScale);

        int size = declIntSize + declFracSize;
        byte[] results = new byte[size];
        fromObject(value, results, 0, declPrec, declScale);
        return results;
    }

    private static int fromObject(BigDecimal value, byte[] dest, int offset, int declPrec, int declScale) {
        final String from = value.toPlainString();
        final int mask = (from.charAt(0) == '-') ? -1 : 0;
        int fromOff = 0;

        if (mask != 0)
            ++fromOff;

        int signSize = mask == 0 ? 0 : 1;
        int periodIndex = from.indexOf('.');
        final int intCnt;
        final int fracCnt;

        if (periodIndex == -1) {
            intCnt = from.length() - signSize;
            fracCnt = 0;
        }
        else {
            intCnt = periodIndex - signSize;
            fracCnt = from.length() - intCnt - 1 - signSize;
        }

        final int intFull = intCnt / DECIMAL_DIGIT_PER;
        final int intPart = intCnt % DECIMAL_DIGIT_PER;
        final int fracFull = fracCnt / DECIMAL_DIGIT_PER;
        final int fracPart = fracCnt % DECIMAL_DIGIT_PER;
        final int intSize = calcBinSize(intCnt);

        final int declIntSize = calcBinSize(declPrec - declScale);
        final int declFracSize = calcBinSize(declScale);

        int toItOff = offset;
        int toEndOff = offset + declIntSize + declFracSize;

        for (int i = 0; (intSize + i) < declIntSize; ++i)
            dest[toItOff++] = (byte) mask;

        int sum = 0;

        // Partial integer
        if (intPart != 0) {
            for (int i = 0; i < intPart; ++i) {
                sum *= 10;
                sum += (from.charAt(fromOff + i) - '0');
            }

            int count = DECIMAL_BYTE_DIGITS[intPart];
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += intPart;
        }

        // Full integers
        for (int i = 0; i < intFull; ++i) {
            sum = 0;

            for (int j = 0; j < DECIMAL_DIGIT_PER; ++j) {
                sum *= 10;
                sum += (from.charAt(fromOff + j) - '0');
            }

            int count = DECIMAL_TYPE_SIZE;
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += DECIMAL_DIGIT_PER;
        }

        // Move past decimal point (or to end)
        ++fromOff;

        // Full fractions
        for (int i = 0; i < fracFull; ++i) {
            sum = 0;

            for (int j = 0; j < DECIMAL_DIGIT_PER; ++j) {
                sum *= 10;
                sum += (from.charAt(fromOff + j) - '0');
            }

            int count = DECIMAL_TYPE_SIZE;
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += DECIMAL_DIGIT_PER;
        }

        // Fraction left over
        if (fracPart != 0) {
            sum = 0;

            for (int i = 0; i < fracPart; ++i) {
                sum *= 10;
                sum += (from.charAt(fromOff + i) - '0');
            }

            int count = DECIMAL_BYTE_DIGITS[fracPart];
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
        }

        while (toItOff < toEndOff)
            dest[toItOff++] = (byte) mask;

        dest[offset] ^= 0x80;

        return declIntSize + declFracSize;
    }

    // for use within this package (for testing)

    /**
     * Decodes bytes representing the decimal value into the given AkibanAppender.
     * @param from the bytes to parse
     * @param location the starting offset within the "from" array
     * @param precision the decimal's precision
     * @param scale the decimal's scale
     * @param appender the StringBuilder to write to
     * @throws NullPointerException if from or appender are null
     * @throws NumberFormatException if the parse failed; the exception's message will be the String that we
     * tried to parse
     */
    public static void decodeToString(byte[] from, int location, int precision, int scale, AkibanAppender appender) {
        final int intCount = precision - scale;
        final int intFull = intCount / DECIMAL_DIGIT_PER;
        final int intPartial = intCount % DECIMAL_DIGIT_PER;
        final int fracFull = scale / DECIMAL_DIGIT_PER;
        final int fracPartial = scale % DECIMAL_DIGIT_PER;

        int curOff = location;

        final int mask = (from[curOff] & 0x80) != 0 ? 0 : -1;

        // Flip high bit during processing
        from[curOff] ^= 0x80;

        if (mask != 0)
            appender.append('-');

        boolean hadOutput = false;
        if (intPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[intPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            curOff += count;
            if (x != 0) {
                hadOutput = true;
                appender.append(x);
            }
        }

        for (int i = 0; i < intFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;

            if (hadOutput) {
                appender.append(String.format("%09d", x));
            } else if (x != 0) {
                hadOutput = true;
                appender.append(x);
            }
        }

        if (fracFull + fracPartial > 0) {
            if (hadOutput) {
                appender.append('.');
            }
            else {
                appender.append("0.");
            }
        }
        else if(!hadOutput)
            appender.append('0');

        for (int i = 0; i < fracFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;
            appender.append(String.format("%09d", x));
        }

        if (fracPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[fracPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            int width = scale - (fracFull * DECIMAL_DIGIT_PER);
            appender.append(String.format("%0" + width + "d", x));
        }

        // Restore high bit
        from[location] ^= 0x80;
    }

    // private methods


    private static int calcBinSize(int digits) {
        int full = digits / DECIMAL_DIGIT_PER;
        int partial = digits % DECIMAL_DIGIT_PER;
        return (full * DECIMAL_TYPE_SIZE) + DECIMAL_BYTE_DIGITS[partial];
    }

    /**
     * Pack an integer, of a given length, in big endian order into a byte array.
     * @param len length of integer
     * @param val value to store in the buffer
     * @param buf destination array to put bytes in
     * @param offset position to start at in buf
     */
    private static void packIntegerByWidth(int len, int val, byte[] buf, int offset) {
        if (len == 1) {
            buf[offset] = (byte) (val);
        } else if (len == 2) {
            buf[offset + 1] = (byte) (val);
            buf[offset]     = (byte) (val >> 8);
        } else if (len == 3) {
            buf[offset + 2] = (byte) (val);
            buf[offset + 1] = (byte) (val >> 8);
            buf[offset]     = (byte) (val >> 16);
        } else if (len == 4) {
            buf[offset + 3] = (byte) (val);
            buf[offset + 2] = (byte) (val >> 8);
            buf[offset + 1] = (byte) (val >> 16);
            buf[offset]     = (byte) (val >> 24);
        } else {
            throw new IllegalArgumentException("Unexpected length " + len);
        }
    }

    /**
     * Unpack a big endian integer, of a given length, from a byte array.
     * @param len length of integer to pull out of buffer
     * @param buf source array to get bytes from
     * @param offset position to start at in buf
     * @return The unpacked integer
     */
    private static int unpackIntegerByWidth(int len, byte[] buf, int offset) {
        if (len == 1) {
            return buf[offset];
        } else if (len == 2) {
            return (buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16) >> 16;
        } else if (len == 3) {
            return (buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16
                    | (buf[offset+2] & 0xFF) << 8) >> 8;
        } else if (len == 4) {
            return buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16
                    | (buf[offset+2] & 0xFF) << 8
                    | (buf[offset+3] & 0xFF);
        }

        throw new IllegalArgumentException("Unexpected length " + len);
    }

    // hidden ctor
    private ConversionHelperBigDecimal() {}

    // consts

    //
    // DECIMAL related defines as specified at:
    // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
    // In short, up to 9 digits get packed into a 4 bytes.
    //
    private static final int DECIMAL_TYPE_SIZE = 4;
    private static final int DECIMAL_DIGIT_PER = 9;
    private static final int DECIMAL_BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
}
