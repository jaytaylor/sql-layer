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

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
import com.akiban.util.AkibanAppender;

abstract class EncodingBase<T> implements Encoding<T> {
    EncodingBase() {
    }

    protected static long getLocation(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
        if (location == 0) {
            throw new EncodingException("invalid location for fieldDef in rowData");
        }
        return location;
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        try {
            sb.append(toObject(fieldDef,rowData));
        } catch (EncodingException e) {
            sb.append("null");
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
