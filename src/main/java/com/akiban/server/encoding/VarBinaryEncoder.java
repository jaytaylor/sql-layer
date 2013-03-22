
package com.akiban.server.encoding;

import java.nio.ByteBuffer;

import com.akiban.server.rowdata.FieldDef;
import com.akiban.util.ByteSource;

public final class VarBinaryEncoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new VarBinaryEncoder();

    private VarBinaryEncoder() {
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
        else if(value instanceof ByteSource) {
            ByteSource bs = (ByteSource)value;
            buffer = ByteBuffer.wrap(bs.byteArray(), bs.byteArrayOffset(), bs.byteArrayLength());
        }
        else {
            throw new IllegalArgumentException("Requires byte[] or ByteBuffer");
        }
        return buffer;
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        final int prefixSize = fieldDef.getPrefixSize();
        final ByteBuffer bb = toByteBuffer(value);
        return bb.remaining() + prefixSize;
    }
}
