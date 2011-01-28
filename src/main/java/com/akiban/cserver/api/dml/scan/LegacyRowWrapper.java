/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.DMLError;

import java.util.Map;

public final class LegacyRowWrapper extends NewRow
{
    // Updates are handled by creating a NiceRow from the RowData and applying updates to the NiceRow.
    // When this happens, rowData is set to null, and is regenerated lazily. If updatedRow is null, then
    // rowData is current.
    private RowData rowData;
    private NiceRow updatedRow;

    // For when a LegacyRowWrapper is being reused, e.g. WriteRowRequest.
    public LegacyRowWrapper()
    {
        this((RowDef) null);
    }

    public LegacyRowWrapper(RowDef rowDef)
    {
        super(rowDef);
        setRowData(null);
    }

    public LegacyRowWrapper(RowData rowData)
    {
        this(rowDef(rowData.getRowDefId()));
        setRowData(rowData);
    }

    public void setRowData(RowData rowData)
    {
        assert
            rowData == null || rowDef == null || rowData.getRowDefId() == rowDef.getRowDefId()
            : String.format("rowData: %s, rowDef: %s", rowData, rowDef);
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
    public TableId getTableId()
    {
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
    public boolean hasValue(ColumnId columnId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(ColumnId columnId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<ColumnId, Object> getFields()
    {
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

    @Override
    public boolean equals(Object o)
    {
        throw new DMLError("Not implemented yet");
    }

    @Override
    public int hashCode()
    {
        throw new DMLError("Not implemented yet");
    }
}
