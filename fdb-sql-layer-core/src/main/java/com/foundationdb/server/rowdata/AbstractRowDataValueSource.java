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

package com.foundationdb.server.rowdata;

import java.math.BigDecimal;
import java.util.UUID;

import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;


abstract class AbstractRowDataValueSource implements ValueSource {

    // ValueSource interface

    @Override
    public TInstance getType() {
        return fieldDef().column().getType();
    }

    @Override
    public boolean hasAnyValue() {
        return true;
    }

    @Override
    public boolean hasRawValue() {
        return ! hasCacheValue();
    }

    @Override
    public boolean hasCacheValue() {
        return fieldDef().column().getType().typeClass() instanceof TString;
    }

    @Override
    public boolean canGetRawValue() {
        return true;
    }

    @Override
    public abstract boolean isNull();

    @Override
    public boolean getBoolean() {
        return extractLong(signage()) != 0;
    }

    @Override
    public boolean getBoolean(boolean defaultValue) {
        return isNull() ? defaultValue : getBoolean();
    }

    @Override
    public byte getInt8() {
        return (byte) extractLong(signage());
    }

    @Override
    public short getInt16() {
        return (short) extractLong(signage());
    }

    @Override
    public char getUInt16() {
        return (char) extractLong(signage());
    }

    @Override
    public int getInt32() {
        return (int) extractLong(signage());
    }

    @Override
    public long getInt64() {
        return extractLong(signage());
    }

    @Override
    public float getFloat() {
        return doGetFloat();
    }

    @Override
    public double getDouble() {
        return doGetDouble();
    }

    @Override
    public byte[] getBytes() {

        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            return null;
        }
        int offset = (int) offsetAndWidth + fieldDef().getPrefixSize();
        int size = (int) (offsetAndWidth >>> 32) - fieldDef().getPrefixSize();
        byte[] bytes = new byte[size];
        System.arraycopy(bytes(), offset, bytes, 0, size);
        return bytes;
    }

    @Override
    public String getString() {
        final long location = getRawOffsetAndWidth();
        return location == 0
                ? null
                : AkServerUtil.decodeMySQLString(bytes(), (int) location, (int) (location >>> 32), fieldDef());
    }

    @Override
    public Object getObject() {
        if (fieldDef().column().getType().typeClass() instanceof TString) {
            return getString();
        } else if (fieldDef().column().getType().typeClass() instanceof TBigDecimal) {
            return getDecimal();
        } else if (fieldDef().column().getType().typeClass() instanceof AkGUID) {
            return getGUID();
        } else {
            assert false : "Unable to get object for type: " + fieldDef();
        }
        return null;
    }

    // for subclasses
    protected abstract long getRawOffsetAndWidth();
    protected abstract byte[] bytes();
    protected abstract FieldDef fieldDef();

    
    
    // for use within this class
    private UUID getGUID() {
        final long location = getRawOffsetAndWidth();
        int offset = (int) location;
        int width = (int) (location >>> 32);
        byte[] bytes = bytes();
        final int prefixSize = fieldDef().getPrefixSize();
        if ( width != 16) {
            throw new IllegalArgumentException(String.format(
                    "GUID must be 16 bytes instead: %d", width));
        }       
        if (location == 0) {
            return null;
        } else {
            return AkGUID.bytesToUUID(bytes, offset+prefixSize);
        }
    }
    
    private BigDecimalWrapperImpl getDecimal() {
        AkibanAppender appender = AkibanAppender.of(new StringBuilder(fieldDef().getMaxStorageSize()));
        ConversionHelperBigDecimal.decodeToString(fieldDef(), bytes(), getRawOffsetAndWidth(), appender);
        String asString = appender.toString();
        assert ! asString.isEmpty();
        try {
            return new BigDecimalWrapperImpl(new BigDecimal(asString));
        } catch (NumberFormatException e) {
            throw new NumberFormatException(asString);
        }
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

    private long extractLong(Signage signage) {
        long offsetAndWidth = getCheckedOffsetAndWidth();
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        if ((signage == Signage.SIGNED) || (width == 8)) {
            return AkServerUtil.getSignedIntegerByWidth(bytes(), offset, width);
        } else {
            assert signage == Signage.UNSIGNED;
            return AkServerUtil.getUnsignedIntegerByWidth(bytes(), offset, width);
        }
    }

    private Signage signage() {
        TClass tclass = fieldDef().column().getType().typeClass();
        if (tclass instanceof MNumeric)
            return ((MNumeric)tclass).isUnsigned() ? Signage.UNSIGNED : Signage.SIGNED;
        else if (tclass == MDateAndTime.YEAR)
            return Signage.UNSIGNED;
        else
            return Signage.SIGNED;
    }

    private long getCheckedOffsetAndWidth() {
        long offsetAndWidth = getRawOffsetAndWidth();
        if (offsetAndWidth == 0) {
            throw new RowDataException("value is null");
        }
        return offsetAndWidth;
    }

    // object state

    private enum Signage {
        SIGNED, UNSIGNED
    }
}
