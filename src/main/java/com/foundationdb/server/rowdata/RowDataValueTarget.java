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

import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.ArgumentValidation;

public final class RowDataValueTarget implements ValueTarget, RowDataTarget {

    @Override
    public void bind(FieldDef fieldDef, byte[] backingBytes, int offset) {
        clear();
        ArgumentValidation.notNull("fieldDef", fieldDef);
        if (offset < 0)
            throw new IllegalArgumentException("offset may not be zero");
        if (offset >= backingBytes.length)
            throw new ArrayIndexOutOfBoundsException(offset);
        this.fieldDef = fieldDef;
        this.bytes = backingBytes;
        this.offset = offset;
    }

    @Override
    public int lastEncodedLength() {
        if (lastEncodedLength < 0) {
            throw new IllegalStateException("no last recorded length available");
        }
        return lastEncodedLength;
    }

    public void putStringBytes(String value) {
        recordEncoded(ConversionHelper.encodeString(value, bytes, offset, fieldDef));
    }

    public RowDataValueTarget() {
        clear();
    }

    public TInstance targetType() {
        return fieldDef.column().getType();
    }

    // ValueTarget interface

    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putNull() {
        setNullBit();
        recordEncoded(0);
    }

    @Override
    public void putDouble(double value) {
        recordEncoded(encodeLong(Double.doubleToLongBits(value)));
    }

    @Override
    public void putFloat(float value) {
        recordEncoded(encodeInt(Float.floatToIntBits(value)));
    }
    
    @Override
    public TInstance getType() {
        return targetType();
    }

    @Override
    public void putBool(boolean value) {
        recordEncoded(encodeLong(value ? 1 : 0));
    }

    @Override
    public void putInt8(byte value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putInt16(short value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putUInt16(char value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putInt32(int value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putInt64(long value) {
        recordEncoded(encodeLong(value));
    }

    @Override
    public void putBytes(byte[] value) {
        recordEncoded(ConversionHelper.putByteArray(value, 0, value.length, bytes, offset, fieldDef));
    }

    @Override
    public void putString(String value, AkCollator collator) {
        recordEncoded(ConversionHelper.encodeString(value, bytes, offset, fieldDef));
    }

    @Override
    public void putObject(Object object) {
        throw new UnsupportedOperationException();
    }

    // private methods
    
    private void recordEncoded(int encodedLength) {
        clear();
        lastEncodedLength = encodedLength;
    }

    private void clear() {
        lastEncodedLength = -1;
        bytes = null;
        offset = -1;
    }

    private int encodeInt(int value) {
        assert INT_STORAGE_SIZE == fieldDef.getMaxStorageSize() : fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes, offset, INT_STORAGE_SIZE, value);
    }

    private int encodeLong(long value) {
        int width = fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(bytes, offset, width, value);
    }

    private void setNullBit() {
        // TODO unloop this
        int target = fieldDef.getFieldIndex();
        int fieldCount = fieldDef.getRowDef().getFieldCount();
        int offsetWithinMap = offset;
        for (int index = 0; index < fieldCount; index += 8) {
            for (int j = index; j < index + 8 && j < fieldCount; j++) {
                if (j == target) {
                    bytes[offsetWithinMap] |= (1 << j - index);
                    return;
                }
            }
            ++offsetWithinMap;
        }
        throw new AssertionError("field not found! " + fieldDef);
    }

    // object state

    private FieldDef fieldDef;
    private int lastEncodedLength;
    private byte bytes[];
    private int offset;

    // consts

    private static final int INT_STORAGE_SIZE = 4;
}
