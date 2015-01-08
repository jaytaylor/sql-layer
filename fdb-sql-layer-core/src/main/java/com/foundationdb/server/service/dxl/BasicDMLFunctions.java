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

package com.foundationdb.server.service.dxl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.aksql.aktypes.*;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BasicDMLFunctions implements DMLFunctions {
    private final static Logger logger = LoggerFactory.getLogger(BasicDMLFunctions.class);

    private final SchemaManager schemaManager;
    private final Store store;
    private final ListenerService listenerService;

    @Inject
    BasicDMLFunctions(SchemaManager schemaManager, Store store, ListenerService listenerService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.listenerService = listenerService;
    }

    /**
     * Determine if a Table can be truncated 'quickly' through the Store interface.
     * This is possible if the entire group can be truncated. Specifically, all other
     * tables in the group must have no rows.
     * @param session Session to operation on
     * @param table Table to determine if a fast truncate is possible on
     * @param descendants <code>true</code> to ignore descendants of
     * <code>table</code> in the check
     * @return true if store.truncateGroup() used, false otherwise
     */
    private boolean canFastTruncate(Session session, Table table, boolean descendants) {
        if(!table.getFullTextIndexes().isEmpty()) {
            return false;
        }
        List<Table> tableList = new ArrayList<>();
        tableList.add(table.getGroup().getRoot());
        while(!tableList.isEmpty()) {
            Table aTable = tableList.remove(tableList.size() - 1);
            if(aTable != table) {
                if(aTable.tableStatus().getRowCount(session) > 0) {
                    return false;
                }
            }
            if((aTable != table) || !descendants) {
                for(Join join : aTable.getChildJoins()) {
                    tableList.add(join.getChild());
                }
            }
        }
        for (Column column : table.getColumns()) {
            if (column.getType().equalsExcludingNullable(AkBlob.INSTANCE.instance(true)))
                return false;
        }
        return true;
    }

    @Override
    public void truncateTable(final Session session, final int tableId)
    {
        truncateTable(session, tableId, false);
    }

    @Override
    public void truncateTable(final Session session, final int tableId, final boolean descendants)
    {
        logger.trace("truncating tableId={}", tableId);
        final AkibanInformationSchema ais = schemaManager.getAis(session);
        final Table table = ais.getTable(tableId);

        if(canFastTruncate(session, table, descendants)) {
            store.truncateGroup(session, table.getGroup());
            // All other tables in the group have no rows. Only need to truncate this table.
            for(TableListener listener : listenerService.getTableListeners()) {
                listener.onTruncate(session, table, true);
            }
            return;
        }

        slowTruncate(session, table, descendants);
    }

    private void slowTruncate(Session session, Table table, boolean descendants) {
        final com.foundationdb.qp.rowtype.Schema schema = SchemaCache.globalSchema(table.getAIS());
        final Set<TableRowType> filterTypes;
        if(descendants) {
            filterTypes = new HashSet<>();
            table.visit(new AbstractVisitor() {
                @Override
                public void visit(Table t) {
                    TableRowType rowType = schema.tableRowType(t);
                    assert rowType != null : t;
                    filterTypes.add(rowType);
                }
            });
        } else {
            filterTypes = Collections.singleton(schema.tableRowType(table));
        }

        // We can't do a "fast truncate" for whatever reason so do so with a full scan.
        Operator plan =
            API.delete_Returning(
                API.filter_Default(
                    API.groupScan_Default(table.getGroup()), filterTypes),
                false
            );

        StoreAdapter adapter = store.createAdapter(session);
        QueryContext context = new SimpleQueryContext(adapter);
        com.foundationdb.qp.operator.Cursor cursor = API.cursor(plan, context, context.createBindings());
        cursor.openTopLevel();
        try {
            Row row;
            do {
                row = cursor.next();
            }
            while(row != null);
        } finally {
            cursor.closeTopLevel();
        }

        for(TableListener listener : listenerService.getTableListeners()) {
            listener.onTruncate(session, table, false);
        }
    }
}
