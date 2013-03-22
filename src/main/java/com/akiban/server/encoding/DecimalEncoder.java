
package com.akiban.server.encoding;

import com.akiban.server.rowdata.FieldDef;

public final class DecimalEncoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new DecimalEncoder();

    private DecimalEncoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }
}
