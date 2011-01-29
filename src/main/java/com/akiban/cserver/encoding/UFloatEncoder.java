package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.Quote;
import com.akiban.cserver.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public final class UFloatEncoder extends EncodingBase<Double> {
    UFloatEncoder() {
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
                          int offset) {
        switch (fieldDef.getMaxStorageSize()) {
            case 4:
                return EncodingUtils.objectToFloat(dest, offset, value, true);
            case 8:
                return EncodingUtils.objectToDouble(dest, offset, value, true);
            default:
                throw new Error("Missing case");
        }
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        long v = rowData.getIntegerValue((int) location,
                (int) (location >>> 32));
        switch (fieldDef.getMaxStorageSize()) {
            case 4:
                return (double)Float.intBitsToFloat((int) v);
            case 8:
                return Double.longBitsToDouble(v);
            default:
                throw new EncodingException("Bad storage size: " + fieldDef.getMaxStorageSize());
        }
    }

    @Override
    public void toString(FieldDef fieldDef, RowData rowData,
                         AkibanAppender sb, final Quote quote) {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return type.fixedSize() && w == 4 || w == 8;
    }

}
