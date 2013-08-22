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

import java.util.Set;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.dml.ColumnSelector;

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
