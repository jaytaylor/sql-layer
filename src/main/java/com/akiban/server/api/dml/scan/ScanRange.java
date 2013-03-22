
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.ColumnSelector;

public interface ScanRange {
    RowData getStart();
    ColumnSelector getStartColumns();
    RowData getEnd();
    ColumnSelector getEndColumns();
    byte[] getColumnBitMap();
    int getTableId();

    /**
     * If this is provided, {@linkplain #getColumnBitMap()} should be ignored, and the behavior should be as if the
     * column bit map were for all of the table's columns.
     * @return
     */
    boolean scanAllColumns();
}
