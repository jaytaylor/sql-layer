package com.akiban.cserver.encoding;

import java.math.BigDecimal;

import com.akiban.ais.model.Type;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
import com.persistit.Key;

public final class DecimalEncoder extends EncodingBase<BigDecimal> {
    public DecimalEncoder() {
    }
    
    //
    // DECIMAL related defines as specified at:
    // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
    // In short, up to 9 digits get packed into a 4 bytes.
    //
    private static final int DECIMAL_TYPE_SIZE = 4;
    private static final int DECIMAL_DIGIT_PER = 9;
    private static final int DECIMAL_BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4,
            4, 4 };


    /**
     * Unpack an int from a byte buffer when stored high bytes first.
     * Corresponds to the defines in found in MySQL's myisampack.h
     *
     * @param len
     *            length to pull out of buffer
     * @param buf
     *            source array to get bytes from
     * @param off
     *            offset to start at in buf
     */
    private static int miUnpack(int len, byte[] buf, int off) {
        int val = 0;

        if (len == 1) {
            val = buf[off];
        } else if (len == 2) {
            val = (buf[off + 0] << 8) | (buf[off + 1] & 0xFF);
        } else if (len == 3) {
            val = (buf[off + 0] << 16) | ((buf[off + 1] & 0xFF) << 8)
                    | (buf[off + 2] & 0xFF);

            if ((buf[off] & 128) != 0)
                val |= (255 << 24);
        } else if (len == 4) {
            val = (buf[off + 0] << 24) | ((buf[off + 1] & 0xFF) << 16)
                    | ((buf[off + 2] & 0xFF) << 8) | (buf[off + 3] & 0xFF);
        }

        return val;
    }

    /**
     * Pack an int into a byte buffer stored with high bytes first.
     * Corresponds to the defines in found in MySQL's myisampack.h
     *
     * @param len
     *            length to put into buffer
     * @param val
     *            value to store in the buffer
     * @param buf
     *            destination array to put bytes in
     * @param offset
     *            offset to start at in buf
     */
    private void miPack(int len, int val, byte[] buf, int offset) {
        if (len == 1) {
            buf[offset] = (byte) (val);
        } else if (len == 2) {
            buf[offset + 1] = (byte) (val);
            buf[offset + 0] = (byte) (val >> 8);
        } else if (len == 3) {
            buf[offset + 2] = (byte) (val);
            buf[offset + 1] = (byte) (val >> 8);
            buf[offset + 0] = (byte) (val >> 16);
        } else if (len == 4) {
            buf[offset + 3] = (byte) (val);
            buf[offset + 2] = (byte) (val >> 8);
            buf[offset + 1] = (byte) (val >> 16);
            buf[offset + 0] = (byte) (val >> 24);
        }
    }

    private int calcBinSize(int digits) {
        int full = digits / DECIMAL_DIGIT_PER;
        int partial = digits % DECIMAL_DIGIT_PER;
        return (full * DECIMAL_TYPE_SIZE) + DECIMAL_BYTE_DIGITS[partial];
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long location = fieldDef.getRowDef().fieldLocation(rowData,
                fieldDef.getFieldIndex());
        if (location == 0) {
            key.append(null);
        } else {
            StringBuilder sb = new StringBuilder();
            toString(fieldDef, rowData, sb, Quote.NONE);

            BigDecimal decimal = new BigDecimal(sb.toString());
            key.append(decimal);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        key.append(value);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        final String from;

        if (value instanceof BigDecimal) {
            from = ((BigDecimal) value).toPlainString();
        } else if (value instanceof Number) {
            from = ((Number) value).toString();
        } else if (value instanceof String) {
            from = (String) value;
        } else if (value == null) {
            from = new String();
        } else {
            throw new IllegalArgumentException(value
                    + " must be a Number or a String");
        }

        final int mask = (from.charAt(0) == '-') ? -1 : 0;
        int fromOff = 0;

        if (mask != 0)
            ++fromOff;

        int signSize = mask == 0 ? 0 : 1;
        int intCnt = from.indexOf('.') - signSize;
        int fracCnt = from.length() - intCnt - 1 - signSize;

        if (intCnt == -1) {
            intCnt = from.length();
            fracCnt = 0;
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
            miPack(count, sum ^ mask, dest, toItOff);

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
            miPack(count, sum ^ mask, dest, toItOff);

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
            miPack(count, sum ^ mask, dest, toItOff);

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
            miPack(count, sum ^ mask, dest, toItOff);

            toItOff += count;
        }

        while (toItOff < toEndOff)
            dest[toItOff++] = (byte) mask;

        dest[offset] ^= 0x80;

        return declIntSize + declFracSize;
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         StringBuilder sb, final Quote quote) {
        decodeAndParse(fieldDef, rowData, sb);
    }

    /**
     * Decodes the field into the given StringBuilder and then returns the parsed BigDecimal.
     * (Always parsing the BigDecimal lets us fail fast if there was a decoding error.)
     * @param fieldDef the field to decode
     * @param rowData the rowdata taht contains the field
     * @param sb the stringbuilder to use
     * @return the parsed BigDecimal
     * @throws NullPointerException if any arguments are null
     * @throws EncodingException if the string can't be parsed to a BigDecimal; the exception's cause will be a
     * NumberFormatException
     */
    private static BigDecimal decodeAndParse(FieldDef fieldDef, RowData rowData, StringBuilder sb) {
        final int precision = fieldDef.getTypeParameter1().intValue();
        final int scale = fieldDef.getTypeParameter2().intValue();
        final long locationAndOffset = fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
        final int location = (int) locationAndOffset;
        final byte[] from = rowData.getBytes();

        try {
            return decodeAndParse(from, location, precision, scale, sb);
        } catch (NumberFormatException e) {
            StringBuilder errSb = new StringBuilder();
            errSb.append("in field[");
            errSb.append(fieldDef.getRowDef().getRowDefId()).append('.').append(fieldDef.getFieldIndex());
            errSb.append(" decimal(");
            errSb.append(fieldDef.getTypeParameter1()).append(',').append(fieldDef.getTypeParameter2());
            errSb.append(")] 0x");
            final int bytesLen = (int) (locationAndOffset >>> 32);
            CServerUtil.hex(errSb, rowData.getBytes(), location, bytesLen);
            errSb.append(": ").append( e.getMessage() );
            throw new EncodingException(errSb.toString(), e);
        }
    }

    /**
     * Decodes bytes into the given StringBuilder and returns the parsed BigDecimal.
     * @param from the bytes to parse
     * @param location the starting offset within the "from" array
     * @param precision the decimal's precision
     * @param scale the decimal's scale
     * @param sb the StringBuilder to write to
     * @return the parsed BigDecimal
     * @throws NullPointerException if from or sb are null
     * @throws NumberFormatException if the parse failed; the exception's message will be the String that we
     * tried to parse
     */
    static BigDecimal decodeAndParse(byte[] from, int location, int precision, int scale, StringBuilder sb) {
        final int sbInitialLen = sb.length();

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
            int x = miUnpack(count, from, curOff) ^ mask;
            curOff += count;
            if (x != 0) {
                hadOutput = true;
                sb.append(x);
            }
        }

        for (int i = 0; i < intFull; ++i) {
            int x = miUnpack(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;

            if (hadOutput) {
                sb.append(String.format("%09d", x));
            } else if (x != 0) {
                hadOutput = true;
                sb.append(x);
            }
        }

        if (fracFull + fracPartial > 0)
            sb.append(hadOutput ? "." : "0.");

        for (int i = 0; i < fracFull; ++i) {
            int x = miUnpack(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;
            sb.append(String.format("%09d", x));
        }

        if (fracPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[fracPartial];
            int x = miUnpack(count, from, curOff) ^ mask;
            int width = scale - (fracFull * DECIMAL_DIGIT_PER);
            sb.append(String.format("%0" + width + "d", x));
        }

        // Restore high bit
        from[location] ^= 0x80;

        final String createdStr = sb.substring(sbInitialLen);

        try {
            return new BigDecimal(createdStr);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(createdStr);
        }
    }

    @Override
    public BigDecimal toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        return decodeAndParse(fieldDef, rowData, new StringBuilder(fieldDef.getMaxStorageSize()));
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
