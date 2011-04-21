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
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.persistit.Key;

public class DoubleEncoder extends EncodingBase<Double> {
    DoubleEncoder() {
    }

    public static double encodeFromObject(Object obj) {
        final double d;
        if(obj == null) {
            d = 0d;
        } else if(obj instanceof Number) {
            d = ((Number)obj).doubleValue();
        } else if (obj instanceof String) {
            d = Double.parseDouble((String)obj);
        } else {
            throw new IllegalArgumentException("Requires Number or String");
        }
        return d;
    }

    public static long fromRowData(RowData rowData, long offsetAndWidth) {
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData.getIntegerValue(offset, width);
    }

    public static long doubleToLong(double d) {
        return Double.doubleToRawLongBits(d);
    }
    

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == STORAGE_SIZE);
    }

    @Override
    public Double toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long offsetAndWidth = getCheckedOffsetAndWidth(fieldDef, rowData);
        final long value = fromRowData(rowData, offsetAndWidth);
        return Double.longBitsToDouble(value);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final double d = encodeFromObject(value);
        final long longBits = doubleToLong(d);
        return EncodingUtils.putInt(dest, offset, longBits, STORAGE_SIZE);
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
            final long offsetAndWidth = getOffsetAndWidth(fieldDef, rowData);
            final long v = fromRowData(rowData, offsetAndWidth);
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        } else {
            final double d = encodeFromObject(value);
            final long longBits = doubleToLong(d);
            key.append(longBits);
        }
    }

    /**
     * See {@link Key#EWIDTH_LONG}
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return 9;
    }

    
    protected static final int STORAGE_SIZE = 8;
}