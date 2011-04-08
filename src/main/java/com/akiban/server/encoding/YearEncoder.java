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
 * Encoder for working with years when stored as a 1 byte int in the
 * range of 0, 1901-2155.  This is how MySQL stores the SQL YEAR type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/year.html
 */
public final class YearEncoder extends EncodingBase<Integer> {
    final int STORAGE_SIZE = 1;

    static int encodeFromObject(Object obj) {
        final int value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof String) {
            final int year = Integer.parseInt((String)obj);
            value = (year == 0) ? 0 : (year - 1900);
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    static String decodeToString(int value) {
        final int year = (value == 0) ? 0 : (1900 + value);
        return String.format("%04d", year);
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
            final int v = 0x000000FF & (int)rowData.getIntegerValue(location, STORAGE_SIZE);
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
        if(value == null) {
            key.append(null);
        } else {
            final int v = 0x000000FF & encodeFromObject(value);
            key.append(v);
        }
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        try {
            final int value = toObject(fieldDef, rowData);
            quote.append(sb, decodeToString(value));
        } catch(EncodingException e) {
            sb.append("null");
        }
    }
}
