package com.akiban.cserver.api.dml.scan;

import java.util.Map;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;

public final class LegacyRowWrapper implements NewRow {
    private RowData rowData;

    public LegacyRowWrapper() {
        setRowData(null);
    }

    public LegacyRowWrapper(RowData rowData) {
        setRowData(rowData);
    }

    public void setRowData(RowData rowData) {
        this.rowData = rowData;
    }

    @Override
    public Object put(ColumnId index, Object object) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableId getTableId() {
        return TableId.of(rowData.getRowDefId());
    }

    @Override
    public Object get(ColumnId columnId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasValue(ColumnId columnId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(ColumnId columnId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<ColumnId, Object> getFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean needsRowDef() {
        return false;
    }

    @Override
    public RowData toRowData(RowDef rowDef) {
        return rowData;
    }
}
