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
import com.akiban.server.RowData;

/**
 * Encoder for working with years when stored as a 1 byte int in the
 * range of 0, 1901-2155.  This is how MySQL stores the SQL YEAR type.
 * See: http://dev.mysql.com/doc/refman/5.5/en/year.html
 */
public final class YearEncoder extends LongEncoderBase {

    @Override
    public long encodeFromObject(Object obj) {
        final int value;
        if(obj == null) {
            value = 0;
        } else if(obj instanceof String) {
            final int year = Integer.parseInt((String)obj);
            value = (year == 0) ? 0 : (year - 1900);
        } else if(obj instanceof Number) {
            value = ((Number)obj).intValue();
        } else {
            throw new IllegalArgumentException("Requires String or Number");
        }
        return value;
    }

    @Override
    public String decodeToString(long value) {
        final long year = (value == 0) ? 0 : (1900 + value);
        return String.format("%04d", year);
    }

    @Override
    public boolean shouldQuoteString() {
        return true;
    }

    @Override
    public boolean validate(Type type) {
        return type.fixedSize() && (type.maxSizeBytes() == 1);
    }

    @Override
    protected long fromRowData(RowData rowData, long offsetAndWidth) {
        // Something wrong about how 1 byte values are stored/retrieved. Work around for now.
        return super.fromRowData(rowData, offsetAndWidth) & 0xFF;
    }
}
