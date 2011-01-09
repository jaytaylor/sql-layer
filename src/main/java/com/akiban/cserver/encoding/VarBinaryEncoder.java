package com.akiban.cserver.encoding;

import java.nio.ByteBuffer;

import com.akiban.ais.model.Type;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
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
    public ByteBuffer toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        int offset = (int) location + fieldDef.getPrefixSize();
        int size = (int) (location >>> 32) - fieldDef.getPrefixSize();

        return ByteBuffer.wrap(rowData.getBytes(), offset, size);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         StringBuilder sb, final Quote quote) {
        final ByteBuffer myBuffer = toObject(fieldDef, rowData);
        sb.append("0x");
        CServerUtil.hex(sb, myBuffer.array(), 0, myBuffer.limit());
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
