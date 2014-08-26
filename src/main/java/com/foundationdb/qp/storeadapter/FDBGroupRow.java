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
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.SparseArray;
import com.persistit.Key;

public class FDBGroupRow extends AbstractRow {
    private final FDBAdapter adapter;
    private final HKeyCache<PersistitHKey> hKeyCache;
    private SparseArray<RowDataValueSource> valueSources;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey currentHKey;


    public FDBGroupRow(FDBAdapter adapter) {
        this.adapter = adapter;
        this.hKeyCache = new HKeyCache<>(adapter);
    }

    public void set(Key key, RowData rowData) {
        this.rowData = rowData;
        RowDef rowDef = adapter.schema().ais().getTable(rowData.getRowDefId()).rowDef();
        row = new LegacyRowWrapper(rowDef, rowData);

        currentHKey = hKeyCache.hKey(rowDef.table());
        if(key != null) {
            currentHKey.copyFrom(key);
        }
    }


    @Override
    public RowType rowType()
    {
        return adapter.schema().tableRowType(rowDef().table());
    }

    @Override
    public ValueSource value(int i) {
        FieldDef fieldDef = rowDef().getFieldDef(i);
        RowData rowData = rowData();
        RowDataValueSource valueSource = ValueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
    }

    @Override
    public PersistitHKey hKey()
    {
        return currentHKey;
    }

    @Override
    public HKey ancestorHKey(Table table)
    {
        PersistitHKey ancestorHKey = hKeyCache.hKey(table);
        currentHKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public boolean containsRealRowOf(Table table)
    {
        return row.getRowDef().table() == table;
    }

    RowDef rowDef()
    {
        if (row != null) {
            return row.getRowDef();
        }
        if (rowData != null) {
            return adapter.schema().ais().getTable(rowData.getRowDefId()).rowDef();
        }
        throw new IllegalStateException("no active row");
    }

    @Override
    public RowData rowData()
    {
        return rowData;
    }

    private RowDataValueSource ValueSource(int i) {
        if (valueSources == null) {
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

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }
}
