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
    protected Class<ByteBuffer> getToObjectClass() {
        return ByteBuffer.class;
    }

    private static ByteBuffer toByteBuffer(Object value) {
        final ByteBuffer buffer;
        if(value == null) {
            buffer = ByteBuffer.wrap(new byte[0]);
        }
        else if(value instanceof byte[]) {
            buffer = ByteBuffer.wrap((byte[])value);
        }
        else if(value instanceof ByteBuffer) {
            buffer = (ByteBuffer)value;
        }
        else {
            throw new IllegalArgumentException("Requires byte[] or ByteBuffer");
        }
        return buffer;
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        final ByteBuffer bb = toByteBuffer(value);
        return EncodingUtils.putByteArray(bb.array(), bb.position(), bb.remaining(), dest, offset, fieldDef);
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        EncodingUtils.toKeyByteArrayEncoding(fieldDef, rowData, key);
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        if(value == null) {
            key.append(null);
        } else {
            ByteBuffer bb = toByteBuffer(value);
            key.appendByteArray(bb.array(), bb.position(), bb.remaining());
        }
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
        byte[] copy = new byte[size];
        System.arraycopy(rowData.getBytes(), offset, copy, 0, size);
        return ByteBuffer.wrap(copy);
    }

    @Override
    public ByteBuffer toObject(Key key) {
        Object o = key.decode();
        if(o != null) {
            byte[] bytes = (byte[])o;
            return ByteBuffer.wrap(bytes, 0, bytes.length);
        }
        return null;
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, final Quote quote) {
        if(rowData.isNull(fieldDef.getFieldIndex())) {
            sb.append("null");
        } else {
            final ByteBuffer bb = toObject(fieldDef, rowData);
            sb.append("0x");
            AkServerUtil.hex(sb, bb.array(), bb.position(), bb.remaining());
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        final int prefixSize = fieldDef.getPrefixSize();
        final ByteBuffer bb = toByteBuffer(value);
        return bb.remaining() + prefixSize;
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return !type.fixedSize() && w < 65536;
    }
}
