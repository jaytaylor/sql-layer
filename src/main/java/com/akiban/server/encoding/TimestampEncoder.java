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

import java.text.ParseException;
import java.util.Date;

import com.akiban.ais.model.Type;
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.persistit.Key;

public final class TimestampEncoder extends EncodingBase<Date> {
    TimestampEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        final long v;
        if (value instanceof String) {
            try {
                final Date date = EncodingUtils.getDateFormat(SDF_DATETIME).parse(
                        (String) value);
                v = (int) (date.getTime() / 1000);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } else if (value instanceof Date) {
            final Date date = (Date) value;
            v = (int) (date.getTime() / 1000);
        } else if (value instanceof Long) {
            v = ((Long) value).longValue();
        } else {
            throw new IllegalArgumentException(
                    "Requires a String or a Date");
        }
        return EncodingUtils.putUInt(dest, offset, v, 4);
    }

    @Override
    public Date toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        final long time = ((long) rowData.getIntegerValue(
                (int) location, 4)) * 1000;
        return new Date(time);
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 4;
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
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
            key.append(dateTimeAsLong((Date) value));
        }
    }

    private long dateTimeAsLong(Date date) {
        // This is NOT what the documentation says:
        // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html.
        // This formula is based on Peter's reverse engineering of mysql
        // packed data.
        return ((date.getYear() + 1900) * LONG_1_E10)
                + ((date.getMonth() + 1) * LONG_1_E8)
                + (date.getDate() * LONG_1_E6)
                + (date.getHours() * LONG_1_E4)
                + (date.getMinutes() * LONG_100) + (date.getSeconds());
    }

    final static String SDF_DATETIME = "yyyy-MM-dd HH:mm:ss";
    private static final long LONG_100 = 100;
    private static final long LONG_1_E4 = LONG_100 * 100;
    private static final long LONG_1_E6 = LONG_1_E4 * 100;
    private static final long LONG_1_E8 = LONG_1_E6 * 100;
    private static final long LONG_1_E10 = LONG_1_E8 * 100;
}
