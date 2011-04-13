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
import com.persistit.Key;

import java.nio.ByteBuffer;

public class StringEncoder extends EncodingBase<String> {
    StringEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        return EncodingUtils.objectToString(value, dest, offset, fieldDef);
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        EncodingUtils.toKeyStringEncoding(fieldDef, rowData, key);
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        key.append(value);
    }

    @Override
    public String toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getCheckedOffsetAndWidth(fieldDef, rowData);
        return rowData.getStringValue((int) location, (int) (location >>> 32), fieldDef);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         AkibanAppender sb, final Quote quote) {
        try {
            final long location = getCheckedOffsetAndWidth(fieldDef, rowData);
            if (sb.canAppendBytes()) {
                ByteBuffer buff = rowData.byteBufferForStringValue((int) location, (int) (location >>> 32), fieldDef);
                quote.append(sb, buff, fieldDef.column().getCharsetAndCollation().charset());
            }
            else {
                String s = rowData.getStringValue((int) location, (int) (location >>> 32), fieldDef);
                quote.append(sb, s);
            }
        } catch (EncodingException e) {
            sb.append("null");
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        final String s = value == null ? "" : value.toString();
        return EncodingUtils.stringByteLength(s) + prefixWidth;
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return !type.fixedSize() && w < 65536 * 3;
    }

}
