package com.akiban.cserver.api.dml.scan;

import java.util.Map;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.service.ServiceManagerImpl;

public final class LegacyRowWrapper implements NewRow {
    private RowDef rowDef;
    private RowData rowData;

    // For when a LegacyRowWrapper is being reused, e.g. WriteRowRequest.
    public LegacyRowWrapper() {
        this.rowDef = null;
        setRowData(null);
    }

    public LegacyRowWrapper(RowDef rowDef) {
        this.rowDef = rowDef;
        setRowData(null);
    }

    public LegacyRowWrapper(RowData rowData) {
        this(rowDef(rowData.getRowDefId()));
        setRowData(rowData);
    }

    public void setRowData(RowData rowData) {
        assert rowData == null || rowData.getRowDefId() == rowDef.getRowDefId();
        this.rowData = rowData;
    }

    @Override
    public Object put(ColumnId index, Object object) {
        // TODO: Do something more efficient in RowData. For now, just create a new RowData.
        // return rowData.fromObject(rowDef, index.getPosition(), object);
        NiceRow niceRow = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        Object replaced = niceRow.put(index, object);
        rowData = niceRow.toRowData();
        return replaced;
    }

    @Override
    public TableId getTableId() {
        return TableId.of(rowData.getRowDefId());
    }

    @Override
    public Object get(ColumnId columnId) {
        return rowData.toObject(rowDef, columnId.getPosition());
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
    public RowData toRowData() {
        return rowData;
    }

    private static RowDef rowDef(int rowDefId)
    {
        RowDefCache rowDefCache = ServiceManagerImpl.get().getStore().getRowDefCache();
        RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        assert rowDef != null : rowDefId;
        return rowDef;
    }
}
