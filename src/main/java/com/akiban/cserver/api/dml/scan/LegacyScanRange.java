package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.LegacyUtils;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.dml.TableDefinitionMismatchException;

public class LegacyScanRange implements ScanRange {
    final RowData start;
    final RowData end;
    final byte[] columnBitMap;
    final int tableId;

    public LegacyScanRange(Integer tableId, RowData start, RowData end, byte[] columnBitMap)
            throws TableDefinitionMismatchException
    {
        Integer rowsTableId = LegacyUtils.matchRowDatas(start, end);
        if ( (rowsTableId != null) && (tableId != null) && (!rowsTableId.equals(tableId)) ) {
            throw new TableDefinitionMismatchException(String.format(
                    "ID<%d> from RowData didn't match given ID <%d>", rowsTableId, tableId));
        }
        this.tableId = tableId == null ? -1 : tableId;
        this.start = start;
        this.end = end;
        this.columnBitMap = columnBitMap;
    }

    @Override
    public RowData getStart(IdResolver ignored) {
        return start;
    }

    @Override
    public RowData getEnd(IdResolver ignored) {
        return end;
    }

    @Override
    public byte[] getColumnBitMap() {
        return columnBitMap;
    }

    @Override
    public int getTableId(IdResolver ignored) {
        return tableId;
    }
}
