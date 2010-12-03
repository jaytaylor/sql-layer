package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.HashSet;
import java.util.Set;

public class ScanAllRange implements ScanRange {

    private final TableId tableId;
    private final byte[] columns;

    public ScanAllRange(TableId tableId, int... columnIds) {
        this.tableId = tableId;
        if (columnIds == null) {
            columns = null;
        }
        else {
            Set<ColumnId> columnIdSet = new HashSet<ColumnId>(columnIds.length);
            for (int colId : columnIds) {
                columnIdSet.add( ColumnId.of(colId) );
            }
            this.columns = ColumnSet.packToLegacy(columnIdSet);
        }
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
