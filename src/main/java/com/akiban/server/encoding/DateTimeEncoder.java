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
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.AkibanAppender;

/**
 * Encoder for working with dates and times when stored as an 8 byte int
 * encoded as (YY*10000 MM*100 + DD)*1000000 + (HH*10000 + MM*100 + SS).
 * This is how MySQL stores the SQL DATETIME type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/datetime.html
 */
public final class DateTimeEncoder extends LongEncoderBase {
    DateTimeEncoder() {
    }
    
    @Override
    public long encodeFromObject(Object obj) {
        final long value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof String) {
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

            value = Integer.parseInt(dateParts[0]) * YEAR_SCALE +
                    Integer.parseInt(dateParts[1]) * MONTH_SCALE +
                    Integer.parseInt(dateParts[2]) * DAY_SCALE +
                    Integer.parseInt(timeParts[0]) * HOUR_SCALE +
                    Integer.parseInt(timeParts[1]) * MIN_SCALE +
                    Integer.parseInt(timeParts[2]) * SEC_SCALE;
        } else if(obj instanceof Number) {
            value = ((Number)obj).longValue();
        } else {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    @Override
    public String decodeToString(long v) {
        final long year = (v / YEAR_SCALE);
        final long month = (v / MONTH_SCALE) % 100;
        final long day = (v / DAY_SCALE) % 100;
        final long hour = (v /HOUR_SCALE) % 100;
        final long minute = (v / MIN_SCALE) % 100;
        final long second = (v / SEC_SCALE) % 100;
        return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                             year, month, day, hour, minute, second);
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 8);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        toStringQuoted(fieldDef, rowData, sb, quote);
    }

    
    static final long DATE_SCALE = 1000000L;
    static final long YEAR_SCALE = 10000L * DATE_SCALE;
    static final long MONTH_SCALE = 100L * DATE_SCALE;
    static final long DAY_SCALE = 1L * DATE_SCALE;
    static final long HOUR_SCALE = 10000L;
    static final long MIN_SCALE = 100L;
    static final long SEC_SCALE = 1L;
}
