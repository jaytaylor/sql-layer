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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Encoder for working with time when stored as a 4 byte int (standard
 * UNIX timestamp). This is how MySQL stores the SQL TIMESTAMP type.
 * <p><b>NOTE</b>: Both input and output is GMT+0 based.</p>
 * See: http://dev.mysql.com/doc/refman/5.5/en/timestamp.html
 * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class TimestampEncoder extends LongEncoderBase {
    final SimpleDateFormat SDF;

    TimestampEncoder() {
        SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SDF.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    
    @Override
    public long encodeFromObject(Object obj) {
        long value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof String) {
            final String s = (String)obj;
            try {
                value = SDF.parse(s).getTime() / 1000;
            } catch(ParseException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    @Override
    public String decodeToString(long value) {
        return SDF.format(new Date(value*1000));
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 4);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        toStringQuoted(fieldDef, rowData, sb, quote);
    }
}
