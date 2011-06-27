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
import com.akiban.server.AkServerUtil;
import com.akiban.server.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public abstract class LongEncoderBase extends EncodingBase<Long> {
    LongEncoderBase() {
    }
    
    @Override
    protected Class<Long> getToObjectClass() {
        return Long.class;
    }

    /**
     * Encode an object to a long. The only strict requirement is null
     * must be handled. In general, at least String and Number should be
     * supported as well.
     * @param obj object to encode
     * @return encoded value
     */
    public long encodeFromObject(Object obj) {
        final long value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof Number) {
            value = ((Number) obj).longValue();
        } else if(obj instanceof String) {
            value = Long.parseLong((String) obj);
        } else {
            throw new IllegalArgumentException("Requires Number or String");
        }
        return value;
    }

    /**
     * Decode a long value into a string representation.
     * @param value encoded value to decode
     * @return string representation
     */
    public String decodeToString(long value) {
        return String.format("%d", value);
    }

    /**
     * Retrieve stored value from an existing RowData.
     * @param rowData RowData to take value from
     * @param offsetAndWidth value containing both offset and width of the field,
     * see {@link RowDef#fieldLocation(RowData, int)}
     * @return encoded value
     */
    protected long fromRowData(RowData rowData, long offsetAndWidth) {
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        return rowData.getIntegerValue(offset, width);
    }

    
    @Override
    public Long toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long offsetAndWidth = getCheckedOffsetAndWidth(fieldDef, rowData);
        return fromRowData(rowData, offsetAndWidth);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final long longValue = encodeFromObject(value);
        final int width = fieldDef.getMaxStorageSize();
        return AkServerUtil.putIntegerByWidth(dest, offset, width, longValue);
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long offsetAndWidth = getOffsetAndWidth(fieldDef, rowData);
        if((int)offsetAndWidth == 0) {
            key.append(null);
        } else {
            long v = fromRowData(rowData, offsetAndWidth);
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        } else {
            long v = encodeFromObject(value);
            key.append(v);
        }
    }

    /**
     * See {@link Key#EWIDTH_LONG}
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return 9;
    }

    /**
     * Purely a helper for the date and time subclasses. Should go away when refactoring encoders.
     */
    protected void toStringQuoted(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        if(rowData.isNull(fieldDef.getFieldIndex())) {
            sb.append("null");
        } else {
            final long offsetAndWidth = getCheckedOffsetAndWidth(fieldDef, rowData);
            final long value = fromRowData(rowData, offsetAndWidth);
            final String string = decodeToString(value);
            quote.append(sb, string);
        }
    }
}
