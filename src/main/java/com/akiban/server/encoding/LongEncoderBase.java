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

import com.akiban.server.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

abstract class LongEncoderBase extends EncodingBase<Long> {

    abstract public long encodeFromObject(Object obj);
    
    abstract public String decodeToString(long value);

    abstract public boolean shouldQuoteString();
    

    @Override
    public Long toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long offsetAndWidth = getLocation(fieldDef, rowData);
        return fromRowData(rowData, offsetAndWidth);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final long longValue = encodeFromObject(value);
        final int width = fieldDef.getMaxStorageSize();
        return EncodingUtils.putInt(dest, offset, longValue, width);
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        final long offsetAndWidth = getLocation(fieldDef, rowData);
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

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        try {
            final Long value = toObject(fieldDef, rowData);
            final String string = decodeToString(value);
            if(shouldQuoteString()) {
                quote.append(sb, string);
            }
            else {
                sb.append(string);
            }
        } catch(EncodingException e) {
            sb.append("null");
        }
    }


    private static long fromRowData(RowData rowData, long offsetAndWidth) {
        final int offset = (int)offsetAndWidth;
        final int width = (int)(offsetAndWidth >>> 32);
        final int shiftSize = 64 - width*8;
        long v = rowData.getIntegerValue(offset, width);
        v <<= shiftSize;
        v >>= shiftSize;
        return v;
    }
}
