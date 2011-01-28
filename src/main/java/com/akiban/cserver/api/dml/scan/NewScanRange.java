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

import java.util.Set;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;

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
        return row.toRowData();
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
