package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {

    private static final int SCAN_FLAGS = ScanFlag.toRowDataFormat(EnumSet.noneOf(ScanFlag.class));

    public ScanAllRequest(TableId tableId, int... columnIds) {
        super(tableId, columnIds);
    }

    @Override
    public int getIndexId() {
        return 0;
    }

    @Override
    public int getScanFlags() {
        return SCAN_FLAGS;
    }
}
