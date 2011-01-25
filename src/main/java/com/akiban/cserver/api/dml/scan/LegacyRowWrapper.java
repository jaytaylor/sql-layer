package com.akiban.cserver.api.dml.scan;

import java.util.Map;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.DMLError;
import com.akiban.cserver.api.dml.NoSuchRowException;
import com.akiban.cserver.service.ServiceManagerImpl;

public final class LegacyRowWrapper implements NewRow {
    private RowDef rowDef;
    // Updates are handled by creating a NiceRow from the RowData and applying updates to the NiceRow.
    // When this happens, rowData is set to null, and is regenerated lazily. If updatedRow is null, then
    // rowData is current.
    private RowData rowData;
    private NiceRow updatedRow;

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
        assert rowData == null || rowDef == null || rowData.getRowDefId() == rowDef.getRowDefId();
        this.rowData = rowData;
        this.updatedRow = null;
    }

    @Override
    public Object put(ColumnId index, Object object)
    {
        if (updatedRow == null) {
            updatedRow = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
            rowData = null;
        }
        return updatedRow.put(index, object);
    }

    @Override
    public TableId getTableId() {
        assert rowData != null || updatedRow != null;
        return updatedRow != null ? updatedRow.getTableId() : TableId.of(rowData.getRowDefId());
    }

    @Override
    public Object get(ColumnId columnId)
    {
        Object object;
        if (rowData == null && updatedRow == null) {
            throw new DMLError("Row state has not been set");
        } else {
            object =
                updatedRow != null
                ? updatedRow.get(columnId)
                : rowData.toObject(rowDef, columnId.getPosition());
        }
        return object;
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
    public RowData toRowData()
    {
        if (rowData == null && updatedRow != null) {
            rowData = updatedRow.toRowData();
            updatedRow = null;
        }
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
