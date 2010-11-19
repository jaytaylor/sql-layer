package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;

// TODO - temporarily we handle just like VARCHAR
public final class TextEncoder extends StringEncoder {
    TextEncoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        final String s = value == null ? "" : value.toString();
        return s.length() + fieldDef.getPrefixSize();
    }

    @Override
    public boolean validate(Type type) {
        long w = type.maxSizeBytes();
        return !type.fixedSize();
    }
}
