/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.encoding;

import java.math.BigDecimal;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public final class DecimalEncoder extends EncodingBase<BigDecimal> {
    DecimalEncoder() {
    }
    
    @Override
    public Class<BigDecimal> getToObjectClass() {
        return BigDecimal.class;
    }

    //
    // DECIMAL related defines as specified at:
    // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
    // In short, up to 9 digits get packed into a 4 bytes.
    //
    private static final int DECIMAL_TYPE_SIZE = 4;
    private static final int DECIMAL_DIGIT_PER = 9;
    private static final int DECIMAL_BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

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

    private static int calcBinSize(int digits) {
        int full = digits / DECIMAL_DIGIT_PER;
        int partial = digits % DECIMAL_DIGIT_PER;
        return (full * DECIMAL_TYPE_SIZE) + DECIMAL_BYTE_DIGITS[partial];
    }

    private static BigDecimal fromObject(Object obj) {
        final BigDecimal value;
        if(obj == null) {
            value = BigDecimal.ZERO;
        }
        else if(obj instanceof BigDecimal) {
            value = (BigDecimal)obj;
        }
        else if(obj instanceof Number || obj instanceof String) {
            value = new BigDecimal(obj.toString());
        }
        else {
            throw new IllegalArgumentException("Must be a Number or String: " + obj);
        }
        return value;
    }
    
    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long location = fieldDef.getRowDef().fieldLocation(rowData,
                fieldDef.getFieldIndex());
        if (location == 0) {
            key.append(null);
        } else {
            AkibanAppender sb = AkibanAppender.of(new StringBuilder());
            toString(fieldDef, rowData, sb, Quote.NONE);

            BigDecimal decimal = new BigDecimal(sb.toString());
            key.append(decimal);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        }
        else {
            BigDecimal dec = fromObject(value);
            key.append(dec);
        }
    }

    /**
     * Note: Only a "good guess". No way to determine how much room
     * key.append(BigDecimal) will take currently.
     */
    @Override
    public long getMaxKeyStorageSize(final Column column) {
        return column.getMaxStorageSize();
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final String from = fromObject(value).toPlainString();
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

        final int declPrec = fieldDef.getTypeParameter1().intValue();
        final int declScale = fieldDef.getTypeParameter2().intValue();
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

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         AkibanAppender sb, final Quote quote) {
        decodeToString(fieldDef, rowData, sb);
    }

    /**
     * Decodes the field from the given RowData into the given AkibanAppender.
     * @param fieldDef the field to decode
     * @param rowData the RowData that contains the field
     * @param sb the appender to use
     * @throws NullPointerException if any arguments are null
     * @throws EncodingException if the string can't be parsed to a BigDecimal; the exception's cause will be a
     */
    private static void decodeToString(FieldDef fieldDef, RowData rowData, AkibanAppender sb) {
        final int precision = fieldDef.getTypeParameter1().intValue();
        final int scale = fieldDef.getTypeParameter2().intValue();
        final long locationAndOffset = fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
        final int location = (int) locationAndOffset;
        final byte[] from = rowData.getBytes();

        try {
            decodeToString(from, location, precision, scale, sb);
        } catch (NumberFormatException e) {
            StringBuilder errSb = new StringBuilder();
            errSb.append("in field[");
            errSb.append(fieldDef.getRowDef().getRowDefId()).append('.').append(fieldDef.getFieldIndex());
            errSb.append(" decimal(");
            errSb.append(fieldDef.getTypeParameter1()).append(',').append(fieldDef.getTypeParameter2());
            errSb.append(")] 0x");
            final int bytesLen = (int) (locationAndOffset >>> 32);
            AkServerUtil.hex(AkibanAppender.of(errSb), rowData.getBytes(), location, bytesLen);
            errSb.append(": ").append( e.getMessage() );
            throw new EncodingException(errSb.toString(), e);
        }
    }

    /**
     * Decodes bytes representing the decimal value into the given AkibanAppender.
     * @param from the bytes to parse
     * @param location the starting offset within the "from" array
     * @param precision the decimal's precision
     * @param scale the decimal's scale
     * @param sb the StringBuilder to write to
     * @throws NullPointerException if from or sb are null
     * @throws NumberFormatException if the parse failed; the exception's message will be the String that we
     * tried to parse
     */
    static void decodeToString(byte[] from, int location, int precision, int scale, AkibanAppender sb) {
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
            sb.append('-');

        boolean hadOutput = false;
        if (intPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[intPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            curOff += count;
            if (x != 0) {
                hadOutput = true;
                sb.append(x);
            }
        }

        for (int i = 0; i < intFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;

            if (hadOutput) {
                sb.append(String.format("%09d", x));
            } else if (x != 0) {
                hadOutput = true;
                sb.append(x);
            }
        }

        if (fracFull + fracPartial > 0) {
            if (hadOutput) {
                sb.append('.');
            }
            else {
                sb.append("0.");
            }
        }
        else if(hadOutput == false)
            sb.append('0');

        for (int i = 0; i < fracFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;
            sb.append(String.format("%09d", x));
        }

        if (fracPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[fracPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            int width = scale - (fracFull * DECIMAL_DIGIT_PER);
            sb.append(String.format("%0" + width + "d", x));
        }

        // Restore high bit
        from[location] ^= 0x80;
    }

    @Override
    public BigDecimal toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        StringBuilder sb = new StringBuilder(fieldDef.getMaxStorageSize());
        decodeToString(fieldDef, rowData, AkibanAppender.of(sb));
        final String createdStr = sb.toString();
        try {
            assert createdStr.isEmpty() == false;
            return new BigDecimal(createdStr);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(createdStr);
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && type.nTypeParameters() == 2;
    }
}
