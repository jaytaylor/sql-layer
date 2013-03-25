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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.entity.changes.EntityParser;
import com.akiban.server.error.ModelBuilderException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.util.JsonUtils;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;

import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;

public class ModelBuilder {
    public static final String ID_COL_NAME = "_id";
    public static final String DATA_COL_NAME = "_data";


    private final SessionService sessionService;
    private final TransactionService txnService;
    private final DDLFunctions ddlFunctions;
    private final Store store;
    private final TreeService treeService;
    private final ConfigurationService configService;
    private final RestDMLService restDMLService;

    public ModelBuilder(SessionService sessionService,
                        TransactionService txnService,
                        DXLService dxlService,
                        Store store,
                        TreeService treeService,
                        ConfigurationService configService,
                        RestDMLService restDMLService) {
        this.sessionService = sessionService;
        this.txnService = txnService;
        this.ddlFunctions = dxlService.ddlFunctions();
        this.store = store;
        this.treeService = treeService;
        this.configService = configService;
        this.restDMLService = restDMLService;

    }

    public void create(TableName tableName) {
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
            createInternal(session, tableName);
            txn.commit();
        }
    }

    public boolean isBuilderTable(TableName tableName) {
        try (Session session = sessionService.createSession()) {
            UserTable curTable = ddlFunctions.getUserTable(session, tableName);
            return isBuilderTable(curTable);
        }
    }

    public void insert(PrintWriter writer, TableName tableName, String data) {
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
            createInternal(session, tableName);
            ObjectNode node = JsonUtils.mapper.createObjectNode();
            // Sequence will fill in ID
            node.put(ModelBuilder.DATA_COL_NAME, data);
            restDMLService.insertNoTxn(session, writer, tableName, node);
            txn.commit();
        }
    }

    public void update(PrintWriter writer, TableName tableName, String id, String data) {
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
            createInternal(session, tableName);
            ObjectNode node = JsonUtils.mapper.createObjectNode();
            node.put(ModelBuilder.ID_COL_NAME, id);
            node.put(ModelBuilder.DATA_COL_NAME, data);
            restDMLService.updateNoTxn(session, writer, tableName, id, node);
            txn.commit();
        }
    }

    public void explode(PrintWriter writer, TableName tableName) throws IOException {
        try (Session session = sessionService.createSession()) {
            AkibanInformationSchema ais = ddlFunctions.getAIS(session);
            Schema schema = SchemaCache.globalSchema(ais);
            UserTable builderTable = ais.getUserTable(tableName);
            if(builderTable == null) {
                throw new NoSuchTableException(tableName);
            }

            StoreAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), store, treeService, session, configService);
            QueryContext queryContext = new SimpleQueryContext(adapter);

            // Just take the data from the last row, laziest way possible
            IndexRowType indexType = schema.indexRowType(builderTable.getPrimaryKey().getIndex());
            UserTableRowType tableType = schema.userTableRowType(builderTable);
            Operator plan = API.ancestorLookup_Default(
                API.indexScan_Default(
                    indexType,
                    true,
                    IndexKeyRange.unbounded(indexType)
                ),
                builderTable.getGroup(),
                indexType,
                Collections.singleton(tableType),
                API.InputPreservationOption.DISCARD_INPUT
            );
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row = cursor.next();
            cursor.destroy();

            if(row == null) {
                throw new ModelBuilderException(tableName, "Entity has no rows");
            }

            ObjectNode node = (ObjectNode)JsonUtils.readTree(row.pvalue(1).getString());
            node.remove(ID_COL_NAME);

            EntityParser parser = new EntityParser();
            NewAISBuilder builder = parser.parse(tableName, node);
            builder.getUserTable().autoIncLong(ID_COL_NAME, 1).pk(ID_COL_NAME);
            UserTable explodedTable = builder.unvalidatedAIS().getUserTable(tableName);
            explodedTable.getColumn(ID_COL_NAME).setNullable(false);

            TableName tempName = new TableName(tableName.getSchemaName(), "___" + tableName.getTableName());
            ddlFunctions.renameTable(session, tableName, tempName);
            ddlFunctions.createTable(session, explodedTable);

            ais = ddlFunctions.getAIS(session);
            adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), store, treeService, session, configService);
            queryContext = new SimpleQueryContext(adapter);
            plan = API.groupScan_Default(ais.getUserTable(tempName).getGroup());
            cursor = API.cursor(plan, queryContext);

            try(CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
                // Copy existing values
                cursor.open();
                while((row = cursor.next()) != null) {
                    node = (ObjectNode)JsonUtils.readTree(row.pvalue(1).getString());
                    node.put(ID_COL_NAME, row.pvalue(0).getInt64());
                    restDMLService.insertNoTxn(session, null, tableName, node);
                }
                cursor.close();
                txn.commit();
            }

            ddlFunctions.dropTable(session, tempName);
        }
    }

    public void implode(TableName tableName) {
        throw new UnsupportedOperationException();
    }


    private void createInternal(Session session, TableName tableName) {
        AkibanInformationSchema curAIS = ddlFunctions.getAIS(session);
        UserTable curTable = curAIS.getUserTable(tableName);
        if(curTable == null) {
            NewAISBuilder builder = AISBBasedBuilder.create();
            builder.userTable(tableName)
                    .autoIncLong(ID_COL_NAME, 1)
                    .colString(DATA_COL_NAME, 65535, true)
                    .pk(ID_COL_NAME);
            ddlFunctions.createTable(session, builder.ais().getUserTable(tableName));
        } else {
            if(!isBuilderTable(curTable)) {
                throw new ModelBuilderException(tableName, "Entity already exists as non-builder table");
            }
        }
    }

    private boolean isBuilderTable(UserTable table) {
        return (table.getColumns().size() == 2) &&
                ID_COL_NAME.equals(table.getColumn(0).getName()) &&
                DATA_COL_NAME.equals(table.getColumn(1).getName());
    }
}
