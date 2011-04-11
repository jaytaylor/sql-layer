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


/**
 * Encoder for working with time when stored as a 3 byte int encoded as
 * HH*3600 + MM*60 + SS. This is how MySQL stores the SQL TIME type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/time.html
 * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class TimeEncoder extends LongEncoderBase {

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

    @Override
    public String decodeToString(long value) {
        final int abs = Math.abs((int)value);
        final int hour = abs / 3600;
        final int minute = (abs - hour*3600) / 60;
        final int second = abs - minute*60 - hour*3600;
        return String.format("%s%02d:%02d:%02d", abs != value ? "-" : "", hour, minute, second);
    }

    @Override
    public boolean shouldQuoteString() {
        return true;
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 3);
    }
}
