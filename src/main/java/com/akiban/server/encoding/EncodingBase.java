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

import com.akiban.server.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

abstract class EncodingBase<T> implements Encoding<T> {
    EncodingBase() {
    }

    protected static long getOffsetAndWidth(FieldDef fieldDef, RowData rowData) {
        return fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
    }

    protected static long getCheckedOffsetAndWidth(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getOffsetAndWidth(fieldDef, rowData);
        if (location == 0) {
            throw new EncodingException("Invalid location for fieldDef in rowData: 0");
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
    public T toObject(Key key) {
        Object o = key.decode();
        return getToObjectClass().cast(o);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
