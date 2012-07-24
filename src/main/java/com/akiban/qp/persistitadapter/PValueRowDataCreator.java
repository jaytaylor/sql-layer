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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

public final class PValueRowDataCreator implements RowDataCreator<PValueSource> {
    @Override
    public PValueSource eval(RowBase row, int f) {
        return row.pvalue(f);
    }

    @Override
    public boolean isNull(PValueSource source) {
        return source.isNull();
    }

    @Override
    public PValueSource createId(long id) {
        return new PValue(id);
    }

    @Override
    public void put(PValueSource source, NewRow into, AkType akType, int f) {
        if (source.isNull()) {
            into.put(f, null);
            return;
        }

        final String s;
        switch (source.getUnderlyingType()) {
        case BOOL:
            s = Boolean.toString(source.getBoolean());
            break;
        case INT_8:
            s = Byte.toString(source.getInt8());
            break;
        case INT_16:
            s = Short.toString(source.getInt16());
            break;
        case UINT_16:
            s = Character.toString(source.getUInt16());
            break;
        case INT_32:
            s = Integer.toString(source.getInt16());
            break;
        case INT_64:
            s = Long.toString(source.getInt64());
            break;
        case FLOAT:
            s = Float.toString(source.getFloat());
            break;
        case DOUBLE:
            s = Double.toString(source.getDouble());
            break;
        case STRING:
            s = source.getString();
            break;
        default:
            throw new AssertionError(source.getUnderlyingType());

        case BYTES:
            // bytes are a special case, in that they're not easily to-stringable
            ByteSource byteSource = new WrappingByteSource(source.getBytes());
            into.put(f, byteSource);
            return;
        }
        into.put(f, s);
    }
}
