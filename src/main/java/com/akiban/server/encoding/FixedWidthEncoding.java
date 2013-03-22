
package com.akiban.server.encoding;

import com.akiban.ais.model.Column;
import com.akiban.server.rowdata.FieldDef;

abstract class FixedWidthEncoding implements Encoding {
    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    @Override
    public long getMaxKeyStorageSize(Column column) {
        return maxKeyStorageSize;
    }

    FixedWidthEncoding(long maxKeyStorageSize) {
        this.maxKeyStorageSize = maxKeyStorageSize;
    }

    private final long maxKeyStorageSize;
}
