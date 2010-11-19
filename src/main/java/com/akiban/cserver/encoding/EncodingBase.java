package com.akiban.cserver.encoding;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;

abstract class EncodingBase<T> implements Encoding<T> {
    EncodingBase() {
    }

    protected static long getLocation(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
        if (location == 0) {
            throw new EncodingException("invalid location for fieldDef in rowData");
        }
        return location;
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData, StringBuilder sb, Quote quote) {
        try {
            sb.append(toObject(fieldDef,rowData));
        } catch (EncodingException e) {
            sb.append("null");
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
