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
 * Encoder for working with time when stored as a 3 byte int encoded as
 * HH*10000 + MM*100 + SS. This is how MySQL stores the SQL TIME type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/time.html
 * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class TimeEncoder extends LongEncoderBase {
    TimeEncoder() {
    }
    
    @Override
    public long encodeFromObject(Object obj) {
        int value = 0;
        if(obj instanceof String) {
            // (-)HH:MM:SS
            String str = (String)obj;
            int mul = 1;
            if(str.length() > 0 && str.charAt(0) == '-') {
                mul = -1;
                str = str.substring(1);
            }
            int hours = 0;
            int minutes = 0;
            int seconds = 0;
            int offset = 0;
            final String values[] = str.split(":");
            switch(values.length) {
                case 3: hours   = Integer.parseInt(values[offset++]); // fall
                case 2: minutes = Integer.parseInt(values[offset++]); // fall
                case 1: seconds = Integer.parseInt(values[offset]);   break;
                default:
                    throw new IllegalArgumentException("Invalid TIME string");
            }
            minutes += seconds/60;
            seconds %= 60;
            hours += minutes/60;
            minutes %= 60;
            value = mul * (hours*HOURS_SCALE + minutes*MINUTES_SCALE + seconds);
        }else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else if(obj != null) {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    @Override
    public String decodeToString(long value) {
        final int abs = Math.abs((int)value);
        final int hour = abs / HOURS_SCALE;
        final int minute = (abs - hour*HOURS_SCALE) / MINUTES_SCALE;
        final int second = abs - hour*HOURS_SCALE - minute*MINUTES_SCALE;
        return String.format("%s%02d:%02d:%02d", abs != value ? "-" : "", hour, minute, second);
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 3);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        toStringQuoted(fieldDef, rowData, sb, quote);
    }

    
    final static int HOURS_SCALE = 10000;
    final static int MINUTES_SCALE = 100;
}
