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
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;

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
    public void put(PValueSource source, NewRow into, FieldDef fieldDef, int f) {

        // TODO efficiency warning
        // NewRow and its users are pretty flexible about types, so let's just convert everything to a String.
        // It's not efficient, but it works.

        if (source.isNull()) {
            into.put(f, null);
            return;
        }
        final Object putObj;
        if (source.hasCacheValue()) {
            putObj = source.getObject();
        }
        else {
            switch (TInstance.pUnderlying(source.getUnderlyingType())) {
            case BOOL:
                putObj = source.getBoolean();
                break;
            case INT_8:
                putObj = source.getInt8();
                break;
            case INT_16:
                putObj = source.getInt16();
                break;
            case UINT_16:
                putObj = source.getUInt16();
                break;
            case INT_32:
                putObj = source.getInt32();
                break;
            case INT_64:
                putObj = source.getInt64();
                break;
            case FLOAT:
                putObj = source.getFloat();
                break;
            case DOUBLE:
                putObj = source.getDouble();
                break;
            case STRING:
                putObj = source.getString();
                break;
            case BYTES:
                putObj = source.getBytes();
                break;
            default:
                throw new AssertionError(source.getUnderlyingType());
            }
        }
        into.put(f, putObj);
    }
}
