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
import com.akiban.server.Quote;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.types.*;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.conversion.*;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

abstract class AbstractRowDataValueSource implements ValueSource {

    // ValueSource interface

    @Override
    public BigDecimal getDecimal() {
        checkState(AkType.DECIMAL);
        AkibanAppender appender = AkibanAppender.of(new StringBuilder(fieldDef().getMaxStorageSize()));
        ConversionHelperBigDecimal.decodeToString(fieldDef(), bytes(), getRawOffsetAndWidth(), appender);
        String asString = appender.toString();
        assert ! asString.isEmpty();
        try {
            return new BigDecimal(asString);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(asString);
        }
    }

    @Override
    public BigInteger getUBigInt() {
        checkState(AkType.U_BIGINT);
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            return null;
        }
        int offset = (int)offsetAndWidth;
        return AkServerUtil.getULong(bytes(), offset);
    }

    @Override
    public ByteSource getVarBinary() {
        checkState(AkType.VARBINARY);
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            return null;
        }
        int offset = (int) offsetAndWidth + fieldDef().getPrefixSize();
        int size = (int) (offsetAndWidth >>> 32) - fieldDef().getPrefixSize();
        return byteSource.wrap(bytes(), offset, size);
    }

    @Override
    public double getDouble() {
        checkState(AkType.DOUBLE);
        return doGetDouble();
    }

    @Override
    public double getUDouble() {
        checkState(AkType.U_DOUBLE);
        return doGetDouble();
    }

    @Override
    public float getFloat() {
        checkState(AkType.FLOAT);
        return doGetFloat();
    }

    @Override
    public float getUFloat() {
        checkState(AkType.U_FLOAT);
        return doGetFloat();
    }

    @Override
    public long getDate() {
        checkState(AkType.DATE);
        return extractLong(Signage.SIGNED);
    }

    @Override
    public long getDateTime() {
        checkState(AkType.DATETIME);
        return extractLong(Signage.SIGNED);
    }

    @Override
    public long getInt() {
        checkState(AkType.INT);
        return extractLong(Signage.SIGNED);
    }

    @Override
    public long getLong() {
        checkState(AkType.LONG);
        return extractLong(Signage.SIGNED);
    }

    @Override
    public long getTime() {
        checkState(AkType.TIME);
        return extractLong(Signage.SIGNED);
    }

    @Override
    public long getTimestamp() {
        checkState(AkType.TIMESTAMP);
        return extractLong(Signage.SIGNED);
    }
    
    @Override
    public long getInterval_Millis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getUInt() {
        checkState(AkType.U_INT);
        return extractLong(Signage.UNSIGNED);
    }

    @Override
    public long getYear() {
        checkState(AkType.YEAR);
        return extractLong(Signage.SIGNED) & 0xFF;
    }

    @Override
    public String getString() {
        checkState(AkType.VARCHAR);
        return doGetString();
    }

    @Override
    public String getText() {
        checkState(AkType.TEXT);
        return doGetString();
    }

    @Override
    public boolean getBool() {
        checkState(AkType.BOOL);
        return extractLong(Signage.SIGNED) != 0;
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        AkType type = getConversionType();
        quote.quote(appender, type);
        if (type == AkType.VARCHAR || type == AkType.TEXT) {
            appendStringField(appender, quote);
        }
        // TODO the rest of this method doesn't give Quote a crack at things.
        // (I think quoting should really be selected at the Appender level, not externally)
        else if (type == AkType.DECIMAL) {
            ConversionHelperBigDecimal.decodeToString(fieldDef(), bytes(), getRawOffsetAndWidth(), appender);
        } else {
            Converters.convert(this, appender.asValueTarget());
        }
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType() {
        return isNull() ? AkType.NULL : fieldDef().getType().akType();
    }

    // for subclasses
    protected abstract long getRawOffsetAndWidth();
    protected abstract byte[] bytes();
    protected abstract FieldDef fieldDef();

    // for use within this class

    private void appendStringField(AkibanAppender appender, Quote quote) {
        try {
            final long location = getCheckedOffsetAndWidth();
            if (appender.canAppendBytes()) {
                ByteBuffer buff = location == 0
                        ? null
                        : AkServerUtil.byteBufferForMySQLString(bytes(), (int)location, (int) (location >>> 32), fieldDef());
                quote.append(appender, buff, fieldDef().column().getCharsetAndCollation().charset());
            }
            else {
                String s = location == 0
                        ? null
                        : AkServerUtil.decodeMySQLString(bytes(), (int)location, (int) (location >>> 32), fieldDef());
                quote.append(appender, s);
            }
        } catch (EncodingException e) {
            quote.append(appender, "<encoding exception! " + e.getMessage() + '>');
        }
    }

    private void checkState(AkType type) {
        ValueSourceHelper.checkType(type, getConversionType());
    }

    private double doGetDouble() {
        long asLong = extractLong(Signage.SIGNED);
        return Double.longBitsToDouble(asLong);
    }

    private float doGetFloat() {
        long asLong = extractLong(Signage.SIGNED);
        int asInt = (int) asLong;
        return Float.intBitsToFloat(asInt);
    }

    private String doGetString() {
        final long location = getRawOffsetAndWidth();
        return location == 0
                ? null
                : AkServerUtil.decodeMySQLString(bytes(), (int) location, (int) (location >>> 32), fieldDef());
    }

    private long extractLong(Signage signage) {
        long offsetAndWidth = getCheckedOffsetAndWidth();
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        if (signage == Signage.SIGNED) {
            return AkServerUtil.getSignedIntegerByWidth(bytes(), offset, width);
        } else {
            assert signage == Signage.UNSIGNED;
            return AkServerUtil.getUnsignedIntegerByWidth(bytes(), offset, width);
        }
    }

    private long getCheckedOffsetAndWidth() {
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            throw new ValueSourceIsNullException();
        }
        return offsetAndWidth;
    }

    // object state
    private final WrappingByteSource byteSource = new WrappingByteSource();

    private enum Signage {
        SIGNED, UNSIGNED
    }
}
