
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
            putObj = source.tInstance().typeClass().formatCachedForNiceRow(source);
        }
        else {
            switch (TInstance.pUnderlying(source.tInstance())) {
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
                throw new AssertionError(source.tInstance());
            }
        }
        into.put(f, putObj);
    }
}
