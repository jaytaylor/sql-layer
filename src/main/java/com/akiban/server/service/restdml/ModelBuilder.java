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
import com.akiban.ais.model.Join;
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
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.service.externaldata.TableRowTracker;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.util.AkibanAppender;
import com.akiban.util.JsonUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;

public class ModelBuilder {
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
            node.put(EntityParser.PK_COL_NAME, id);
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
            node.remove(EntityParser.PK_COL_NAME);

            EntityParser parser = new EntityParser(true);
            NewAISBuilder builder = parser.parse(tableName, node);
            UserTable explodedTable = builder.unvalidatedAIS().getUserTable(tableName);

            TableName tempName = new TableName(tableName.getSchemaName(), "__" + tableName.getTableName());
            ddlFunctions.renameTable(session, tableName, tempName);
            parser.create(ddlFunctions, session, explodedTable);

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
                    node.put(EntityParser.PK_COL_NAME, row.pvalue(0).getInt64());
                    removeUnknownFields(explodedTable, node);
                    restDMLService.insertNoTxn(session, null, tableName, node);
                }
                cursor.close();
                txn.commit();
            }

            ddlFunctions.dropTable(session, tempName);
        }
    }

    public void implode(PrintWriter writer, TableName tableName) {
        try (Session session = sessionService.createSession()) {
            UserTable curTable = ddlFunctions.getUserTable(session, tableName);
            if(!curTable.isRoot()) {
                throw new ModelBuilderException(tableName, "Cannot implode non-root table");
            }

            if(isBuilderTable(curTable)) {
                throw new ModelBuilderException(tableName, "Table is already imploded");
            }

            // Rename current
            TableName tempName = new TableName(tableName.getSchemaName(), "__" + tableName.getTableName());
            ddlFunctions.renameTable(session, tableName, tempName);

            // Create new
            createInternal(session, tableName);

            // Scan old into new
            UserTable newTable = ddlFunctions.getUserTable(session, tableName);
            AkibanInformationSchema ais = ddlFunctions.getAIS(session);
            PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(ais), store, treeService, session, configService);
            SimpleQueryContext queryContext = new SimpleQueryContext(adapter);
            Operator plan = API.groupScan_Default(ais.getUserTable(tempName).getGroup());
            Cursor cursor = API.cursor(plan, queryContext);
            ImplodeTracker tracker = new ImplodeTracker(newTable, -1);
            JsonRowWriter rowWriter = new JsonRowWriter(tracker);
            rowWriter.writeRows(cursor, AkibanAppender.of(tracker.getStringBuilder()), "", new JsonRowWriter.WriteTableRow());

            // drop old
            ddlFunctions.dropTable(session, tempName);
        }
    }


    private void createInternal(Session session, TableName tableName) {
        AkibanInformationSchema curAIS = ddlFunctions.getAIS(session);
        UserTable curTable = curAIS.getUserTable(tableName);
        if(curTable == null) {
            NewAISBuilder builder = AISBBasedBuilder.create();
            builder.userTable(tableName)
                    .autoIncLong(EntityParser.PK_COL_NAME, 1)
                    .colString(DATA_COL_NAME, 65535, true)
                    .pk(EntityParser.PK_COL_NAME);
            ddlFunctions.createTable(session, builder.ais().getUserTable(tableName));
        } else {
            if(!isBuilderTable(curTable)) {
                throw new ModelBuilderException(tableName, "Entity already exists as non-builder table");
            }
        }
    }


    private static UserTable findChildTable(UserTable parent, String childName) {
        for(Join j : parent.getChildJoins()) {
            UserTable child = j.getChild();
            if(child.getName().getTableName().equals(childName)) {
                return child;
            }
        }
        return null;
    }

    private static void removeUnknownFields(UserTable table, JsonNode node) {
        if(node.isArray()) {
            Iterator<JsonNode> it = node.getElements();
            while(it.hasNext()) {
                removeUnknownFields(table, it.next());
            }
        }
        else {
            Iterator<Map.Entry<String,JsonNode>> fieldIt = node.getFields();
            while(fieldIt.hasNext()) {
                Map.Entry<String,JsonNode> pair = fieldIt.next();
                String itName = pair.getKey();
                JsonNode itNode = pair.getValue();
                if(itNode.isContainerNode()) {
                    UserTable child = findChildTable(table, itName);
                    if(child != null) {
                        removeUnknownFields(child, itNode);
                    } else {
                        fieldIt.remove();
                    }
                } else {
                    if(table.getColumn(itName) == null) {
                        fieldIt.remove();
                    }
                }
            }
        }
    }

    private static boolean isBuilderTable(UserTable table) {
        return (table.getColumns().size() == 2) &&
                EntityParser.PK_COL_NAME.equals(table.getColumn(0).getName()) &&
                DATA_COL_NAME.equals(table.getColumn(1).getName());
    }

    private class ImplodeTracker extends TableRowTracker {
        private final StringBuilder builder;
        private final UserTable rootTable;
        private int curDepth = 0;

        public ImplodeTracker(UserTable table, int addlDepth) {
            super(table, addlDepth);
            this.builder = new StringBuilder();
            this.rootTable = table;
        }

        public StringBuilder getStringBuilder() {
            return builder;
        }

        @Override
        public void pushRowType() {
            super.pushRowType();
            ++curDepth;
        }

        @Override
        public void popRowType() {
            super.popRowType();
            if(--curDepth == 0) {
                String json = (builder.charAt(0) == ',') ? builder.substring(1) : builder.toString();
                ModelBuilder.this.insert(null, rootTable.getName(), json);
                builder.setLength(0);
            }
        }
    }
}
