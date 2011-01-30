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

package com.akiban.cserver.encoding;

import java.text.ParseException;
import java.util.Date;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
import com.persistit.Key;

public class DateTimeEncoder extends EncodingBase<Date> {
    DateTimeEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        final long v;
        if (value instanceof String) {
            try {
                final Date date = EncodingUtils.getDateFormat(SDF_DATETIME).parse(
                        (String) value);
                v = dateTimeAsLong(date);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } else if (value instanceof Date) {
            final Date date = (Date) value;
            v = dateTimeAsLong(date);
        } else if (value instanceof Long) {
            v = ((Long) value).longValue();
        } else {
            throw new IllegalArgumentException(
                    "Requires a String or a Date");
        }
        return EncodingUtils.putUInt(dest, offset, v, 8);
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

    @Override
    public Date toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);

        final long v = rowData.getIntegerValue((int) location, 8);
        // Note: reverse engineered; this does not match documentation
        // at
        // http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
        final int year = (int) (v / LONG_1_E10);
        final int month = (int) ((v / LONG_1_E8) % 100);
        final int day = (int) ((v / LONG_1_E6) % 100);
        final int hour = (int) ((v / LONG_1_E4) % 100);
        final int minute = (int) ((v / LONG_100) % 100);
        final int second = (int) (v % LONG_100);
        return new Date(year - 1900, month - 1, day, hour,
                minute, second);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, StringBuilder sb, final Quote quote) {
        try {
            final Date date = toObject(fieldDef, rowData);
            quote.append(sb, EncodingUtils.getDateFormat(SDF_DATETIME).format(date));
        }
        catch (EncodingException e) {
            sb.append("null");
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 8;
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

    private static final long LONG_100 = 100;
    private static final long LONG_1_E4 = LONG_100 * 100;
    private static final long LONG_1_E6 = LONG_1_E4 * 100;
    private static final long LONG_1_E8 = LONG_1_E6 * 100;
    private static final long LONG_1_E10 = LONG_1_E8 * 100;

    final static String SDF_DATETIME = "yyyy-MM-dd HH:mm:ss";
}
