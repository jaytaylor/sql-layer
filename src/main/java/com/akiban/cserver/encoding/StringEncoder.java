package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
import com.persistit.Key;

public class StringEncoder extends EncodingBase<String> {
    StringEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        return EncodingUtils.objectToString(value, dest, offset, fieldDef);
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        EncodingUtils.toKeyStringEncoding(fieldDef, rowData, key);
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        key.append(value);
    }

    @Override
    public String toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        return rowData.getStringValue((int) location, (int) (location >>> 32), fieldDef);
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         StringBuilder sb, final Quote quote) {
        try {
            quote.append(sb, toObject(fieldDef, rowData));
        } catch (EncodingException e) {
            sb.append("null");
        }
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        final String s = value == null ? "" : value.toString();
        return EncodingUtils.stringByteLength(s) + prefixWidth;
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return !type.fixedSize() && w < 65536 * 3;
    }

}
