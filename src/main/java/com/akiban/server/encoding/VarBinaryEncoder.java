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

import java.nio.ByteBuffer;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public final class VarBinaryEncoder extends EncodingBase<ByteBuffer>{
    VarBinaryEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        if (!(value instanceof byte[])) {
            throw new IllegalArgumentException(value
                    + " must be a byte array");
        }
        return EncodingUtils.putByteArray((byte[]) value, dest, offset, fieldDef);
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        EncodingUtils.toKeyByteArrayEncoding(fieldDef, rowData, key);
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        key.append(value);
    }

    @Override
    public long getMaxKeyStorageSize(Column column) {
        return column.getMaxStorageSize();
    }

    @Override
    public ByteBuffer toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getCheckedOffsetAndWidth(fieldDef, rowData);
        int offset = (int) location + fieldDef.getPrefixSize();
        int size = (int) (location >>> 32) - fieldDef.getPrefixSize();

        return ByteBuffer.wrap(rowData.getBytes(), offset, size);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         AkibanAppender sb, final Quote quote) {
        final ByteBuffer myBuffer = toObject(fieldDef, rowData);
        sb.append("0x");
        AkServerUtil.hex(sb, myBuffer.array(), 0, myBuffer.limit());
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        return ((byte[]) value).length + prefixWidth;
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return !type.fixedSize() && w < 65536;
    }

}
