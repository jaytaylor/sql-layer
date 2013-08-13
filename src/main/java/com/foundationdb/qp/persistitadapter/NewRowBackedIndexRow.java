/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.persistitadapter;

import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;

public class NewRowBackedIndexRow implements RowBase
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
        this.sources = new FromObjectValueSource[rowType.nFields()];
        for (int f = 0; f < rowType.nFields(); f++) {
            this.sources[f] = new FromObjectValueSource();
        }
    }

    // RowBase interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        FromObjectValueSource source = sources[fieldPos];
        if (row.isColumnNull(fieldPos)) {
            source.setNull();
        } else {
            source.setReflectively(row.get(fieldPos));
        }
        return source;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey ancestorHKey(UserTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ancestorOf(RowBase that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PValueSource pvalue(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        FromObjectValueSource source = sources[fieldPos];
        if (row.isColumnNull(fieldPos)) {
            source.setNull();
        } else {
            source.setReflectively(row.get(fieldPos));
        }
        return PValueSources.fromValueSource(source, rowType.typeInstanceAt(fieldPos));
    }

    @Override
    public int compareTo(RowBase row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // Object state

    private final NewRow row;
    private final RowType rowType;
    private final TableIndex index;
    private final FromObjectValueSource[] sources;
}
