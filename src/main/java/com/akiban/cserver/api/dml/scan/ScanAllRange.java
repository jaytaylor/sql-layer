package com.akiban.cserver.api.dml.scan;

import java.util.Set;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;

public class ScanAllRange implements ScanRange {

    private final TableId tableId;
    private final byte[] columns;

    public ScanAllRange(TableId tableId, Set<ColumnId> columnIds) {
        this.tableId = tableId;
        this.columns = columnIds == null ? null : ColumnSet.packToLegacy(columnIds);
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
        if (scanAllColumns()) {
            throw new UnsupportedOperationException("scanAllColumns() is true!");
        }
        return columns;
    }

    @Override
    public int getTableIdInt(IdResolver idResolver) throws NoSuchTableException {
        return tableId.getTableId(idResolver);
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public boolean scanAllColumns() {
        return columns == null;
    }
}
