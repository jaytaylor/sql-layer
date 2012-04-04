/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
