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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionSourceAppendHelper;
import com.akiban.server.types.SourceIsNullException;
import com.akiban.util.AkibanAppender;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public final class FieldDefConversionSource extends FieldDefConversionBase implements ConversionSource {

    // ConversionSource interface

    @Override
    public boolean isNull() {
        return (rowData().isNull(fieldDef().getFieldIndex()));
    }

    @Override
    public BigDecimal getDecimal() {
        AkibanAppender appender = AkibanAppender.of(new StringBuilder(fieldDef().getMaxStorageSize()));
        ConversionHelperBigDecimal.decodeToString(fieldDef(), rowData(), appender);
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
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            return null;
        }
        int offset = (int)offsetAndWidth;
        int width = (int)(offsetAndWidth >>> 32);
        return rowData().getUnsignedLongValue(offset, width);
    }

    @Override
    public ByteBuffer getVarBinary() {
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            return null;
        }
        int offset = (int) offsetAndWidth + fieldDef().getPrefixSize();
        int size = (int) (offsetAndWidth >>> 32) - fieldDef().getPrefixSize();
        byte[] copy = new byte[size];
        System.arraycopy(rowData().getBytes(), offset, copy, 0, size);
        return ByteBuffer.wrap(copy);
    }

    @Override
    public double getDouble() {
        long asLong = extractLong();
        return Double.longBitsToDouble(asLong);
    }

    @Override
    public double getUDouble() {
        return getDouble();
    }

    @Override
    public float getFloat() {
        long asLong = extractLong();
        int asInt = (int) asLong;
        return Float.intBitsToFloat(asInt);
    }

    @Override
    public float getUFloat() {
        return getFloat();
    }

    @Override
    public long getDate() {
        return extractLong();
    }

    @Override
    public long getDateTime() {
        return extractLong();
    }

    @Override
    public long getInt() {
        return extractLong();
    }

    @Override
    public long getLong() {
        return extractLong();
    }

    @Override
    public long getTime() {
        return extractLong();
    }

    @Override
    public long getTimestamp() {
        return extractLong();
    }

    @Override
    public long getUInt() {
        return extractLong();
    }

    @Override
    public long getYear() {
        return extractLong();
    }

    @Override
    public String getString() {
        final long location = getRawOffsetAndWidth();
        return location == 0
                ? null
                : rowData().getStringValue((int) location, (int) (location >>> 32), fieldDef());
    }

    @Override
    public String getText() {
        return getString();
    }

    @Override
    public void appendAsString(AkibanAppender appender) {
        appendHelper.source(this).appendTo(appender);
    }

    // for use within this class
    // Stolen from the Encoding classes

    private long getRawOffsetAndWidth() {
        return fieldDef().getRowDef().fieldLocation(rowData(), fieldDef().getFieldIndex());
    }
    
    private long extractLong() {
        long offsetAndWidth = getCheckedOffsetAndWidth();
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData().getIntegerValue(offset, width);
    }

    private long getCheckedOffsetAndWidth() {
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            throw new SourceIsNullException();
        }
        return offsetAndWidth;
    }

    // object state
    private final ConversionSourceAppendHelper appendHelper = new ConversionSourceAppendHelper() {
        @Override
        protected AkType akType() {
            return fieldDef().column().getType().akType();
        }

        @Override
        protected void appendDecimal(AkibanAppender appender) {
            ConversionHelperBigDecimal.decodeToString(fieldDef(), rowData(), appender);
        }
    };
}
