package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

public interface ScanRange {
    RowData getStart(IdResolver idResolver) throws NoSuchTableException;
    RowData getEnd(IdResolver idResolver) throws NoSuchTableException;
    byte[] getColumnBitMap();
    int getTableIdInt(IdResolver idResolver) throws NoSuchTableException;
    TableId getTableId();
}
