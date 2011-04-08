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
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

/**
 * Encoder for working with dates when stored as a 3 byte int using
 * the encoding of DD + MM×32 + YYYY×512. This is how MySQL stores the
 * SQL DATE type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class DateEncoder extends EncodingBase<Integer> {
    final int STORAGE_SIZE = 3;

    static int encodeFromObject(Object obj) {
        final int value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof String) {
            // YYYY-MM-DD
            final String values[] = ((String)obj).split("-");
            int y = 0, m = 0, d = 0;
            switch(values.length) {
                case 3: d = Integer.parseInt(values[2]); // fall
                case 2: m = Integer.parseInt(values[1]); // fall
                case 1: y = Integer.parseInt(values[0]); break;
                default:
                    throw new IllegalArgumentException("Invalid date string");
            }
            value = d + m*32 + y*512;
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    static String decodeToString(int value) {
        final int year = value / 512;
        final int month = (value / 32) % 16;
        final int day = value % 32;
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == STORAGE_SIZE);
    }

    @Override
    public Integer toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final int location = (int)getLocation(fieldDef, rowData);
        return (int)rowData.getIntegerValue(location, STORAGE_SIZE);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
        final int longValue = encodeFromObject(value);
        return EncodingUtils.putInt(dest, offset, longValue, STORAGE_SIZE);
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final int location = (int)fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
        if(location == 0) {
            key.append(null);
        } else {
            final int value = (int)rowData.getIntegerValue(location, STORAGE_SIZE);
            key.append(value);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
        if(value == null) {
            key.append(null);
        } else {
            final int v = 0x00FFFFFF & encodeFromObject(value);
            key.append(v);
        }
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        try {
            final int value = toObject(fieldDef, rowData);
            sb.append(decodeToString(value));
        } catch(EncodingException e) {
            sb.append("null");
        }
    }
}
