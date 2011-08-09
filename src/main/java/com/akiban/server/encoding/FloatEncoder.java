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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.persistit.Key;

public class FloatEncoder extends EncodingBase<Float> {
    FloatEncoder() {
    }

    @Override
    public Class<Float> getToObjectClass() {
        return Float.class;
    }

    public static int encodeFromObject(Object obj) {
        final float f;
        if(obj == null) {
            f = 0f;
        } else if(obj instanceof Number) {
            f = ((Number)obj).floatValue();
        } else if (obj instanceof String) {
            f = Float.parseFloat((String)obj);
        } else {
            throw new IllegalArgumentException("Requires Number or String");
        }
        return Float.floatToIntBits(f);
    }

    public static float decodeFromBits(int bits) {
        return Float.intBitsToFloat(bits);
    }

    private static int fromRowData(RowData rowData, long offsetAndWidth) {
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        long value = rowData.getIntegerValue(offset, width);
        value <<= 32;
        value >>= 32;
        return (int)value;
    }
    

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == STORAGE_SIZE);
    }

    @Override
    public Float toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long offsetAndWidth = getCheckedOffsetAndWidth(fieldDef, rowData);
        final int value = fromRowData(rowData, offsetAndWidth);
        return decodeFromBits(value);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final int intBits = encodeFromObject(value);
        return AkServerUtil.putIntegerByWidth(dest, offset, STORAGE_SIZE, intBits);
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        if(rowData.isNull(fieldDef.getFieldIndex())) {
            key.append(null);
        } else {
            final float f = toObject(fieldDef, rowData);
            key.append(f);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        } else {
            final int bits = encodeFromObject(value);
            final float f = decodeFromBits(bits);
            key.append(f);
        }
    }

    /**
     * See {@link Key#EWIDTH_INT}
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return 5;
    }

    
    protected static final int STORAGE_SIZE = 4;
}