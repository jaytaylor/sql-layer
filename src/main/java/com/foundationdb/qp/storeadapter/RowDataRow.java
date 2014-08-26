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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.SparseArray;

/** Simple RowData wrapper. Does not provide {@link #hKey()}. */
public class RowDataRow extends AbstractRow
{
    private final TableRowType rowType;
    private final RowData rowData;
    private SparseArray<RowDataValueSource> valueSources;

    public RowDataRow(TableRowType rowType, RowData rowData) {
        assert (rowType.typeId() == rowData.getRowDefId());
        this.rowType = rowType;
        this.rowData = rowData;
    }

    @Override
    public TableRowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    @Override
    public ValueSource value(int i) {
        FieldDef fieldDef = rowType.table().rowDef().getFieldDef(i);
        RowDataValueSource valueSource = valueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
    }

    @Override
    public RowData rowData() {
        return rowData;
    }

    private RowDataValueSource valueSource(int i) {
        if(valueSources == null) {
            valueSources = new SparseArray<RowDataValueSource>()
            {
                @Override
                protected RowDataValueSource initialValue() {
                    return new RowDataValueSource();
                }
            };
        }
        return valueSources.get(i);
    }
}
