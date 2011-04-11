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
 * Encoder for working with time when stored as a 4 byte int (standard
 * UNIX timestamp). This is how MySQL stores the SQL TIMESTAMP type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/timestamp.html
 * and  http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
 */
public final class TimestampEncoder extends LongEncoderBase {
    TimestampEncoder() {
    }
    
    @Override
    public long encodeFromObject(Object obj) {
        int value = 0;
        if(obj instanceof String) {
            value = Integer.parseInt((String)obj);
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else if(obj != null) {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    @Override
    public String decodeToString(long value) {
        return String.format("%d", value);
    }

    @Override
    public boolean shouldQuoteString() {
        return false;
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 4);
    }
}
