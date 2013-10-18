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

package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Limit;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.storeadapter.PersistitGroupRow;
import com.foundationdb.qp.storeadapter.PersistitRowLimit;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.test.it.PersistitITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.test.pt.PTBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QPProfilePTBase extends PTBase
{
    // TODO: Remove this need. See newGroupRow() below.

    @Override
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return PersistitITBase.doBind(super.serviceBindingsProvider());
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    PersistitAdapter persistitAdapter(Schema schema) {
        PersistitStore store = (PersistitStore)store();
        return store.createAdapter(session(), schema);
    }

    protected Group group(int tableId)
    {
        return getRowDef(tableId).table().getGroup();
    }

    protected Table table(int tableId)
    {
        return getRowDef(tableId).table();
    }

    protected IndexRowType indexType(int tableId, String... searchIndexColumnNamesArray)
    {
        Table table = table(tableId);
        for (Index index : table.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.tableRowType(table(tableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected ColumnSelector columnSelector(final Index index)
    {
        return new ColumnSelector()
        {
            @Override
            public boolean includesColumn(int columnPosition)
            {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    if (column.getPosition() == columnPosition) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    protected Row row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    protected Row row(int tableId, Object... values /* alternating field position and value */)
    {
        NewRow niceRow = createNewRow(tableId);
        int i = 0;
        while (i < values.length) {
            int position = (Integer) values[i++];
            Object value = values[i++];
            niceRow.put(position, value);
        }
        return PersistitGroupRow.newPersistitGroupRow(adapter, niceRow.toRowData());
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.openTopLevel();
            count = 0;
            List<Row> actualRows = new ArrayList<>(); // So that result is viewable in debugger
            Row actualRow;
            while ((actualRow = cursor.next()) != null) {
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.closeTopLevel();
        }
        assertEquals(expected.length, count);
    }

    protected static final Limit NO_LIMIT = new PersistitRowLimit(ScanLimit.NONE);

    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
}
