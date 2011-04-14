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

public final class FloatEncoder extends EncodingBase<Double> {
    FloatEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        switch (fieldDef.getMaxStorageSize()) {
            case 4:
                return EncodingUtils.objectToFloat(dest, offset, value, false);
            case 8:
                return EncodingUtils.objectToDouble(dest, offset, value, false);
            default:
                throw new Error("Missing case");
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxKeyStorageSize(Column column) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getCheckedOffsetAndWidth(fieldDef, rowData);

        long v = rowData.getIntegerValue((int) location,
                (int) (location >>> 32));
        switch (fieldDef.getMaxStorageSize()) {
            case 4:
                return (double)Float.intBitsToFloat((int) v);
            case 8:
                return Double.longBitsToDouble(v);
            default:
                throw new EncodingException("Bad storage size " + fieldDef.getMaxStorageSize());
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 4 || w == 8;
    }

}
