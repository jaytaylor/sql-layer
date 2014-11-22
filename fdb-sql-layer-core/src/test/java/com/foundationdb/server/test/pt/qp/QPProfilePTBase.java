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
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.test.it.PersistitITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.test.pt.PTBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    PersistitAdapter persistitAdapter() {
        PersistitStore store = (PersistitStore)store();
        return store.createAdapter(session());
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

    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
}
