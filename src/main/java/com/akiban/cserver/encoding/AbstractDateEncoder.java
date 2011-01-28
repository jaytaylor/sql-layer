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

abstract class AbstractDateEncoder extends EncodingBase<Date> {
    AbstractDateEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        final int v;
        if (value instanceof String) {
            try {
                final Date date = EncodingUtils.getDateFormat(EncodingUtils.SDF_DATE).parse(
                        (String) value);
                v = dateAsInt(date);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } else if (value instanceof Date) {
            final Date date = (Date) value;
            v = dateAsInt(date);
        } else if (value instanceof Long) {
            v = ((Long) value).intValue();
        } else {
            throw new IllegalArgumentException(
                    "Requires a String or a Date");
        }
        return EncodingUtils.putUInt(dest, offset, v, 3);
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
            key.append(dateAsInt((Date) value));
        }
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         StringBuilder sb, final Quote quote) {
        final Date date;
        try {
            date = toObject(fieldDef, rowData);
        } catch (EncodingException e) {
            sb.append("null");
            return;
        }
        quote.append(sb, EncodingUtils.getDateFormat(EncodingUtils.SDF_DATE).format(date));
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    private int dateAsInt(Date date) {
        // This formula is specified here:
        // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
        // Note, the Date#getMonth() method returns a 0-based month, whereas
        // MySQL is 1-based.
        return ((date.getYear() + 1900) * 32 * 16)
                + ((date.getMonth() + 1) * 32) + date.getDate();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 3 || w == 4 || w == 8;
    }
}
