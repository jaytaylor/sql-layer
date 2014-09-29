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

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TypeDeclarationException;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

public class NewRowBackedIndexRow implements Row
{
    // Object interface

    @Override
    public String toString()
    {
        return row.toString();
    }

    // NewRowBackedIndexRow interface

    NewRowBackedIndexRow(RowType rowType, NewRow row, TableIndex index) {
        this.rowType = rowType;
        this.row = row;
        this.index = index;
    }

    // Row interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey ancestorHKey(Table table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ancestorOf(Row that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsRealRowOf(Table table) {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource value(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        Object value = row.get(fieldPos);
        TInstance type = rowType.typeAt(fieldPos);
        if (DEBUG_ROWTYPE && type == null && value != null) {
            throw new RowType.InconsistentRowTypeException(i, value);
        }
        return ValueSources.valuefromObject(value, type);
    }

    @Override
    public int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // Object state

    private final NewRow row;
    private final RowType rowType;
    private final TableIndex index;

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }
}
