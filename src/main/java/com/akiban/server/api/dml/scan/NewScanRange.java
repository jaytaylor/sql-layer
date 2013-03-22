
package com.akiban.server.api.dml.scan;

import java.util.Set;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.ColumnSelector;

public class NewScanRange implements ScanRange {
    protected final int tableId;
    protected final Set<Integer> columns;
    protected final Predicate predicate;

    public NewScanRange(int tableId, Set<Integer> columns, Predicate predicate) {
        this.tableId = tableId;
        this.columns = columns;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public byte[] getColumnSetBytes() {
        return ColumnSet.packToLegacy(columns);
    }

    @Override
    public RowData getStart() {
        return convert(predicate.getStartRow());
    }

    @Override
    public ColumnSelector getStartColumns() {
        return null;
    }

    @Override
    public RowData getEnd() {
        return convert(predicate.getEndRow());
    }

    @Override
    public ColumnSelector getEndColumns() {
        return null;
    }

    private RowData convert(NewRow row) {
        return row.toRowData();
    }

    @Override
    public byte[] getColumnBitMap() {
        return ColumnSet.packToLegacy(columns);
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
