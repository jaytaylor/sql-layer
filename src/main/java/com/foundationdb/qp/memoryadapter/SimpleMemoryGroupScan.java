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

package com.foundationdb.qp.memoryadapter;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.ValuesRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;

import java.util.Iterator;

public abstract class SimpleMemoryGroupScan<T> implements MemoryGroupCursor.GroupScan {

    protected abstract Object[] createRow(T data, int hiddenPk);

    @Override
    public Row next() {
        if (!iterator.hasNext())
            return null;
        Object[] rowContents = createRow(iterator.next(), ++hiddenPk);
        return new ValuesRow(rowType, rowContents);
    }

    @Override
    public void close() {
        // nothing
    }

    public SimpleMemoryGroupScan(MemoryAdapter adapter, TableName tableName, Iterator<? extends T> iterator) {
        this.iterator = iterator;
        this.rowType = adapter.schema().tableRowType(adapter.schema().ais().getTable(tableName));
    }

    private final Iterator<? extends T> iterator;
    private final RowType rowType;
    private int hiddenPk = 0;
}
