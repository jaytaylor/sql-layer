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
 * Encoder for working with dates and times when stored as an 8 byte int
 * encoded as (YY*10000 MM*100 + DD)*1000000 + (HH*10000 + MM*100 + SS).
 * This is how MySQL stores the SQL DATETIME type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/datetime.html
 */
public final class DateTimeEncoder extends EncodingBase<Long> {
    static final int STORAGE_SIZE = 8;
    static final long DATE_SCALE = 1000000L;
    static final long YEAR_SCALE = 10000L * DATE_SCALE;
    static final long MONTH_SCALE = 100L * DATE_SCALE;
    static final long DAY_SCALE = 1L * DATE_SCALE;
    static final long HOUR_SCALE = 10000L;
    static final long MIN_SCALE = 100L;
    static final long SEC_SCALE = 1L;

    static long encodeFromObject(Object obj) {
        long value = 0;
        if(obj instanceof String) {
            final String parts[] = ((String)obj).split(" ");
            if(parts.length != 2) {
                throw new IllegalArgumentException("Invalid DATETIME string");
            }

            final String dateParts[] = parts[0].split("-");
            if(dateParts.length != 3) {
                throw new IllegalArgumentException("Invalid DATE portion");
            }

            final String timeParts[] = parts[1].split(":");
            if(timeParts.length != 3) {
                throw new IllegalArgumentException("Invalid TIME portion");
            }

            value += Integer.parseInt(dateParts[0]) * YEAR_SCALE +
                     Integer.parseInt(dateParts[1]) * MONTH_SCALE +
                     Integer.parseInt(dateParts[2]) * DAY_SCALE +
                     Integer.parseInt(timeParts[0]) * HOUR_SCALE +
                     Integer.parseInt(timeParts[1]) * MIN_SCALE +
                     Integer.parseInt(timeParts[2]) * SEC_SCALE;
        } else if(obj instanceof Number) {
            value = ((Number)obj).longValue();
        } else if(obj != null) {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    static String decodeToString(long v) {
        final long year = (v / YEAR_SCALE);
        final long month = (v / MONTH_SCALE) % 100;
        final long day = (v / DAY_SCALE) % 100;
        final long hour = (v /HOUR_SCALE) % 100;
        final long minute = (v / MIN_SCALE) % 100;
        final long second = (v / SEC_SCALE) % 100;
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             year, month, day, hour, minute, second);
    }

    static long fromRowData(RowData rowData, int location) {
        return rowData.getIntegerValue(location, STORAGE_SIZE);
    }
    

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == STORAGE_SIZE);
    }

    @Override
    public Long toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final int location = (int)getLocation(fieldDef, rowData);
        return fromRowData(rowData, location);
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
        final long longValue = encodeFromObject(value);
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
            long v = fromRowData(rowData, location);
            key.append(v);
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        assert fieldDef.getMaxStorageSize() == STORAGE_SIZE : fieldDef;
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
            final long value = toObject(fieldDef, rowData);
            quote.append(sb, decodeToString(value));
        } catch(EncodingException e) {
            sb.append("null");
        }
    }
}
