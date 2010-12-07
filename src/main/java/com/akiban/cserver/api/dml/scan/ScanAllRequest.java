package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;

import java.util.EnumSet;
import java.util.Set;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {

    private static final int SCAN_FLAGS = ScanFlag.toRowDataFormat(EnumSet.noneOf(ScanFlag.class));

    public ScanAllRequest(TableId tableId, Set<ColumnId> columnIds) {
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
