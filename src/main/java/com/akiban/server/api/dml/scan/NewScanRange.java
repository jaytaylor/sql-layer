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

package com.akiban.server.api.dml.scan;

import java.util.Set;

import com.akiban.server.RowData;
import com.akiban.server.api.common.NoSuchTableException;
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
    public RowData getStart() throws NoSuchTableException {
        return convert(predicate.getStartRow());
    }

    @Override
    public ColumnSelector getStartColumns() {
        return null;
    }

    @Override
    public RowData getEnd() throws NoSuchTableException {
        return convert(predicate.getEndRow());
    }

    @Override
    public ColumnSelector getEndColumns() {
        return null;
    }

    private RowData convert(NewRow row) throws NoSuchTableException {
        return row.toRowData();
    }

    @Override
    public byte[] getColumnBitMap() {
        return ColumnSet.packToLegacy(columns);
    }

    @Override
    public int getTableId() throws NoSuchTableException {
        return tableId;
    }

    @Override
    public boolean scanAllColumns() {
        return false;
    }
}
