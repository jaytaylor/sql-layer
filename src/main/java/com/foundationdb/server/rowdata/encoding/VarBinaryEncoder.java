/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.rowdata.encoding;

import java.nio.ByteBuffer;

import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.util.ByteSource;

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
