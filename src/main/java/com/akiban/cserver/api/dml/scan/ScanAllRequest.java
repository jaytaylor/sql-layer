package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class ScanAllRequest implements ScanRequest {

    private final TableId tableId;
    private final byte[] columns;

    private static final int SCAN_FLAGS = ScanFlag.toRowDataFormat(EnumSet.noneOf(ScanFlag.class));

    public ScanAllRequest(TableId tableId, int... columnIds) {
        this.tableId = tableId;
        Set<ColumnId> columnIdSet = new HashSet<ColumnId>(columnIds.length);
        for (int colId : columnIds) {
            columnIdSet.add( new ColumnId(colId) );
        }
        this.columns = ColumnSet.packToLegacy(columnIdSet);
    }

    @Override
    public int getIndexId() {
        return 0;
    }

    @Override
    public int getScanFlags() {
        return SCAN_FLAGS;
    }

    @Override
    public RowData getStart(IdResolver idResolver) {
        return null;
    }

    @Override
    public RowData getEnd(IdResolver idResolver) {
        return null;
    }

    @Override
    public byte[] getColumnBitMap() {
        return columns;
    }

    @Override
    public int getTableId(IdResolver idResolver) throws NoSuchTableException {
        return tableId.getTableId(idResolver);
    }
}
