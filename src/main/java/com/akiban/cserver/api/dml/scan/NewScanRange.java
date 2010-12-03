package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.Set;

public class NewScanRange implements ScanRange {
    protected final TableId tableId;
    protected final Set<ColumnId> columns;
    protected final Predicate predicate;

    public NewScanRange(TableId tableId, Set<ColumnId> columns, Predicate predicate) {
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
    public RowData getStart(IdResolver idResolver) throws NoSuchTableException {
        return convert(predicate.getStartRow(), idResolver);
    }

    @Override
    public RowData getEnd(IdResolver idResolver) throws NoSuchTableException {
        return convert(predicate.getEndRow(), idResolver);
    }

    private RowData convert(NewRow row, IdResolver idResolver) throws NoSuchTableException {
        RowDef rowDef = null;
        if (row.needsRowDef()) {
            rowDef = idResolver.getRowDef(tableId);
        }
        return row.toRowData(rowDef);
    }

    @Override
    public byte[] getColumnBitMap() {
        return ColumnSet.packToLegacy(columns);
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
        return false;
    }
}
