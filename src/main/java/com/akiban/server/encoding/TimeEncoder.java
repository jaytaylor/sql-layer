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
 * Encoder for working with time when stored as a 3 byte int encoded as
 * HH*3600 + MM*60 + SS. This is how MySQL stores the SQL TIME type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/time.html
 * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class TimeEncoder extends EncodingBase<Integer> {
    static final int STORAGE_SIZE = 3;

    static int encodeFromObject(Object obj) {
        int value = 0;
        if(obj instanceof String) {
            // (-)HH:MM:SS
            String str = (String)obj;
            int mul = 1;
            if(str.length() > 0 && str.charAt(0) == '-') {
                mul = -1;
                str = str.substring(1);
            }
            final String values[] = str.split(":");
            final int scaleArray[] = {3600, 60, 1};
            int offset;
            switch(values.length) {
                case 3: offset = 0; break;
                case 2: offset = 1; break;
                case 1: offset = 2; break;
                default:
                    throw new IllegalArgumentException("Invalid TIME string");
            }
            for(int i = 0; i < values.length; ++i, ++offset) {
                value += Integer.parseInt(values[i]) * scaleArray[offset];
            }
            value *= mul;
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else if(obj != null) {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    static String decodeToString(int value) {
        final int abs = Math.abs(value);
        final int hour = abs / 3600;
        final int minute = (abs - hour*3600) / 60;
        final int second = abs - minute*60 - hour*3600;
        return String.format("%s%02d:%02d:%02d", abs != value ? "-" : "", hour, minute, second);
    }

    static int fromRowData(RowData rowData, int location) {
        final int shiftSize = 64 - STORAGE_SIZE * 8;
        long v = rowData.getIntegerValue(location, STORAGE_SIZE);
        v <<= shiftSize;
        v >>= shiftSize;
        return (int)v;
    }
    

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == STORAGE_SIZE);
    }

    @Override
    public Integer toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final int location = (int)getLocation(fieldDef, rowData);
        return fromRowData(rowData, location);
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
        final int location = (int)getLocation(fieldDef, rowData);
        if(location == 0) {
            key.append(null);
        } else {
            int v = fromRowData(rowData, location);
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
        if(value == null) {
            key.append(null);
        } else {
            int v = encodeFromObject(value);
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
