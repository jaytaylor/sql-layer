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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataPValueSource;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.SparseArray;
import com.persistit.Key;

public class FDBGroupRow extends AbstractRow {
    private final FDBAdapter adapter;
    private final HKeyCache<PersistitHKey> hKeyCache;
    private SparseArray<RowDataPValueSource> pvalueSources;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey currentHKey;


    public FDBGroupRow(FDBAdapter adapter) {
        this.adapter = adapter;
        this.hKeyCache = new HKeyCache<>(adapter);
    }

    public void set(Key key, RowData rowData) {
        this.rowData = rowData;
        RowDef rowDef = adapter.schema().ais().getUserTable(rowData.getRowDefId()).rowDef();
        row = new LegacyRowWrapper(rowDef, rowData);

        currentHKey = hKeyCache.hKey(rowDef.userTable());
        if(key != null) {
            currentHKey.copyFrom(key);
            rowData.hKey(key);
        }
    }


    @Override
    public RowType rowType()
    {
        return adapter.schema().userTableRowType(rowDef().userTable());
    }

    @Override
    public ValueSource eval(int i)
    {
        throw new UnsupportedOperationException("types3 off");
    }

    @Override
    public PValueSource pvalue(int i) {
        FieldDef fieldDef = rowDef().getFieldDef(i);
        RowData rowData = rowData();
        RowDataPValueSource valueSource = pValueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
    }

    @Override
    public PersistitHKey hKey()
    {
        return currentHKey;
    }

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        PersistitHKey ancestorHKey = hKeyCache.hKey(table);
        currentHKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        return row.getRowDef().userTable() == userTable;
    }

    RowDef rowDef()
    {
        if (row != null) {
            return row.getRowDef();
        }
        if (rowData != null) {
            return adapter.schema().ais().getUserTable(rowData.getRowDefId()).rowDef();
        }
        throw new IllegalStateException("no active row");
    }

    @Override
    public RowData rowData()
    {
        return rowData;
    }

    private RowDataPValueSource pValueSource(int i) {
        if (pvalueSources == null) {
            pvalueSources = new SparseArray<RowDataPValueSource>()
            {
                @Override
                protected RowDataPValueSource initialValue() {
                    return new RowDataPValueSource();
                }
            };
        }
        return pvalueSources.get(i);
    }
}
