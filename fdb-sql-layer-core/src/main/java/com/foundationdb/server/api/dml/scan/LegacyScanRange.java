/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.api.dml.scan;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.LegacyUtils;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.error.TableDefinitionMismatchException;

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
