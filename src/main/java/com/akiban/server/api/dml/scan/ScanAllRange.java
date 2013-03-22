
package com.akiban.server.api.dml.scan;

import java.util.Set;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.ColumnSelector;

public class ScanAllRange implements ScanRange {

    private final int tableId;
    private final byte[] columns;

    public ScanAllRange(int tableId, Set<Integer> columnIds) {
        this.tableId = tableId;
        this.columns = columnIds == null ? null : ColumnSet.packToLegacy(columnIds);
    }

    @Override
    public RowData getStart() {
        return null;
    }

    @Override
    public ColumnSelector getStartColumns() {
        return null;
    }

    @Override
    public RowData getEnd() {
        return null;
    }

    @Override
    public ColumnSelector getEndColumns() {
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
    public int getTableId(){
        return tableId;
    }

    @Override
    public boolean scanAllColumns() {
        return columns == null;
    }
}
