package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;

public interface ScanRange {
    RowData getStart(IdResolver idResolver) throws NoSuchTableException;
    RowData getEnd(IdResolver idResolver) throws NoSuchTableException;
    byte[] getColumnBitMap();
    int getTableIdInt(IdResolver idResolver) throws NoSuchTableException;
    TableId getTableId();

    /**
     * If this is provided, {@linkplain #getColumnBitMap()} should be ignored, and the behavior should be as if the
     * column bit map were for all of the table's columns.
     * @return
     */
    boolean scanAllColumns();
}
