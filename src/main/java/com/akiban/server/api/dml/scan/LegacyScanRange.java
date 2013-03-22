
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.LegacyUtils;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.TableDefinitionMismatchException;

public class LegacyScanRange implements ScanRange {
    final RowData start;
    final ColumnSelector startColumns;
    final RowData end;
    final ColumnSelector endColumns;
    final byte[] columnBitMap;
    final int tableId;

    public LegacyScanRange(Integer tableId, RowData start, ColumnSelector startColumns,
                           RowData end, ColumnSelector endColumns, byte[] columnBitMap)
    {
        Integer rowsTableId = LegacyUtils.matchRowDatas(start, end);
        if ( (rowsTableId != null) && (tableId != null) && (!rowsTableId.equals(tableId)) ) {
            throw new TableDefinitionMismatchException (rowsTableId, tableId);
        }
        this.tableId = tableId == null ? -1 : tableId;
        this.start = start;
        this.startColumns = startColumns;
        this.end = end;
        this.endColumns = endColumns;
        this.columnBitMap = columnBitMap;
    }

    @Override
    public RowData getStart() {
        return start;
    }

    @Override
    public ColumnSelector getStartColumns() {
        return startColumns;
    }

    @Override
    public RowData getEnd() {
        return end;
    }

    @Override
    public ColumnSelector getEndColumns() {
        return endColumns;
    }

    @Override
    public byte[] getColumnBitMap() {
        return columnBitMap;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public boolean scanAllColumns() {
        return false;
    }
}
