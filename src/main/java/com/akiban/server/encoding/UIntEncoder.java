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

import com.akiban.ais.model.Type;
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.persistit.Key;

public final class UIntEncoder extends EncodingBase<Long> {
    UIntEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        return EncodingUtils.objectToInt(dest, offset, value,
                fieldDef.getMaxStorageSize(), true);
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long location = fieldDef.getRowDef().fieldLocation(rowData,
                fieldDef.getFieldIndex());
        if (location == 0) {
            key.append(null);
        } else {
            long v = rowData.getIntegerValue((int) location,
                    (int) (location >>> 32));
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if (value == null) {
            key.append(null);
        } else {
            long v = ((Number) value).longValue();
            v <<= 64 - (fieldDef.getMaxStorageSize() * 8);
            v >>= 64 - (fieldDef.getMaxStorageSize() * 8);
            // TODO: unsigned long
            key.append(v);
        }
    }

    @Override
    public Long toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        return rowData.getIntegerValue((int) location, (int) (location >>> 32));
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 1 || w == 2 || w == 3 || w == 4
                || w == 8;
    }

}
