package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.persistit.Key;

class UnsupportedTypeEncoder<T> extends EncodingBase<T> {
    private final String name;
    UnsupportedTypeEncoder(String name) {
        this.name = name;
    }

    private UnsupportedOperationException complaint() {
        return new UnsupportedOperationException(name + " is an unsupported type");
    }

    @Override
    public boolean validate(Type type) {
        throw complaint();
    }

    @Override
    public T toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        throw complaint();
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        throw complaint();
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        throw complaint();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        throw complaint();
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        throw complaint();
    }
}
