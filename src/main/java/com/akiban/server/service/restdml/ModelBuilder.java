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
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.entity.changes.EntityParser;
import com.akiban.server.error.ModelBuilderException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.service.externaldata.TableRowTracker;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.sql.optimizer.rule.PlanGenerator;
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
            // Find the last row
            Row row = getLastRow(session, tableName);
            if(row == null) {
                throw new ModelBuilderException(tableName, "Entity has no rows");
            }

            // Parse that row
            ObjectNode node = (ObjectNode)JsonUtils.readTree(row.pvalue(1).getString());
            node.remove(EntityParser.PK_COL_NAME);

            // Move builder out of the way and create exploded table(s)
            EntityParser parser = new EntityParser(true);
            UserTable explodedTable = parser.parse(tableName, node);
            TableName tempName = new TableName(tableName.getSchemaName(), "__" + tableName.getTableName());
            ddlFunctions.renameTable(session, tableName, tempName);
            parser.create(ddlFunctions, session, explodedTable);

            // Convert builder rows
            int convertedRows = 0;
            try(CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
                Cursor cursor = groupScanCursor(session, tempName);
                cursor.open();
                try {
                    while((row = cursor.next()) != null) {
                        node = (ObjectNode)JsonUtils.readTree(row.pvalue(1).getString());
                        node.put(EntityParser.PK_COL_NAME, row.pvalue(0).getInt64());
                        removeUnknownFields(explodedTable, node);
                        restDMLService.insertNoTxn(session, null, tableName, node);
                        ++convertedRows;
                    }
                    cursor.close();
                } finally {
                    cursor.destroy();
                }
                txn.commit();
            }

            // Remove builder table
            ddlFunctions.dropTable(session, tempName);

            ObjectNode summaryNode = JsonUtils.mapper.createObjectNode();
            summaryNode.put("exploded", tableName.getTableName());
            summaryNode.put("rows", convertedRows);
            writer.append(summaryNode.toString());
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
            UserTable oldTable = ddlFunctions.getUserTable(session, tempName);
            UserTable newTable = ddlFunctions.getUserTable(session, tableName);
            ImplodeTracker tracker = new ImplodeTracker(newTable, oldTable, -1);
            Cursor cursor = groupScanCursor(session, tempName);
            try {
                JsonRowWriter rowWriter = new JsonRowWriter(tracker);
                rowWriter.writeRows(cursor, AkibanAppender.of(tracker.getStringBuilder()), "", new JsonRowWriter.WriteTableRow());
            } finally {
                cursor.destroy();
            }

            // drop old
            ddlFunctions.dropGroup(session, tempName);

            ObjectNode summaryNode = JsonUtils.mapper.createObjectNode();
            summaryNode.put("imploded", tableName.getTableName());
            summaryNode.put("rows", tracker.getRowCount());
            writer.append(summaryNode.toString());
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

    private Row getLastRow(Session session, TableName tableName) {
        UserTable table = ddlFunctions.getUserTable(session, tableName);
        Schema schema = SchemaCache.globalSchema(table.getAIS());
        IndexRowType indexRowType = schema.indexRowType(table.getPrimaryKey().getIndex());
        Operator plan = API.ancestorLookup_Default(
                API.indexScan_Default(
                        indexRowType,
                        true,
                        IndexKeyRange.unbounded(indexRowType)
                ),
                table.getGroup(),
                indexRowType,
                Collections.singleton(schema.userTableRowType(table)),
                API.InputPreservationOption.DISCARD_INPUT
        );
        StoreAdapter adapter = new PersistitAdapter(schema, store, treeService, session, configService);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        Cursor cursor = API.cursor(plan, queryContext);
        cursor.open();
        try {
            return cursor.next();
        } finally {
            cursor.destroy();
        }
    }

    private Cursor groupScanCursor(Session session, TableName tableName) {
        UserTable table = ddlFunctions.getUserTable(session, tableName);
        Operator plan = PlanGenerator.generateScanPlan(table.getAIS(), table);
        PersistitAdapter adapter = new PersistitAdapter(SchemaCache.globalSchema(table.getAIS()), store, treeService, session, configService);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        return API.cursor(plan, queryContext);
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
        private final UserTable outTable;
        private int rowCount = 0;
        private int curDepth = 0;

        public ImplodeTracker(UserTable outTable, UserTable inTable, int addlDepth) {
            super(inTable, addlDepth);
            assert inTable.isRoot() : "Expected root: "  + inTable;
            this.builder = new StringBuilder();
            this.outTable = outTable;
        }

        public StringBuilder getStringBuilder() {
            return builder;
        }

        public int getRowCount() {
            return rowCount;
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
                ModelBuilder.this.insert(null, outTable.getName(), json);
                builder.setLength(0);
                ++rowCount;
            }
        }
    }
}
