
package com.akiban.server.encoding;

import com.akiban.ais.model.Column;

abstract class VariableWidthEncoding implements Encoding {
    /**
     * Note: Only a "good guess" for BigDecimal. No way to determine how much room
     * key.append(BigDecimal) will take currently.
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return column.getMaxStorageSize();
    }

}
