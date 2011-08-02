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

package com.akiban.server.rowdata;

import com.akiban.server.AkServerUtil;
import com.akiban.server.types.SourceConversionException;
import com.akiban.util.AkibanAppender;

final class ConversionHelperBigDecimal {

    // "public" methods (though still only available within-package)

    /**
     * Decodes the field from the given RowData into the given StringBuilder.
     * @param fieldDef the fieldDef whose type is a decimal
     * @param rowData the RowData from which to extract the BigDecimal
     * @param sb the appender to use
     * @throws NullPointerException if any arguments are null
     * @throws SourceConversionException if the string can't be parsed to a BigDecimal
     */
    public static void decodeToString(FieldDef fieldDef, RowData rowData, StringBuilder sb) {
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
            throw new SourceConversionException(errSb.toString(), e);
        }
    }

    // for use within this package (for testing)

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
    static void decodeToString(byte[] from, int location, int precision, int scale, StringBuilder sb) {
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
        else if(!hadOutput)
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

    // private methods

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
