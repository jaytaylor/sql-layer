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
package com.foundationdb.rest.dml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidChildCollectionException;
import com.foundationdb.server.error.KeyColumnMissingException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.externaldata.ExternalDataServiceImpl;
import com.foundationdb.server.service.externaldata.JsonRowWriter;
import com.foundationdb.server.service.externaldata.PlanGenerator;
import com.foundationdb.server.service.externaldata.TableRowTracker;
import com.foundationdb.server.service.externaldata.JsonRowWriter.WriteCapturePKRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.util.AkibanAppender;
import com.fasterxml.jackson.databind.JsonNode;

public class UpsertProcessor extends DMLProcessor {

    private final InsertProcessor insertProcessor;
    private final ExternalDataService extDataService;
    private FormatOptions options;
    
    public UpsertProcessor(Store store, SchemaManager schemaManager,
            TypesRegistryService typesRegistryService,
            InsertProcessor insertProcessor,
            ExternalDataService extDataService,
            FormatOptions options) {
        super(store, schemaManager, typesRegistryService);
        this.insertProcessor = insertProcessor;
        this.extDataService = extDataService;
        this.options = options;
    }

    
    public String processUpsert(Session session, AkibanInformationSchema ais, TableName tableName, JsonNode node) {
        ProcessContext context = new ProcessContext (ais, session, tableName);
        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);

        processContainer (node, appender, context);
        
        return appender.toString();

    }
    
    private void processContainer (JsonNode node, AkibanAppender appender, ProcessContext context) {
        boolean first = true;
        
        if (node.isObject()) {
            processRow (node, appender, context);
        } else if (node.isArray()) {
            appender.append('[');
            for (JsonNode arrayElement : node) {
                if (first) { 
                    first = false;
                } else {
                    appender.append(',');
                }
                if (arrayElement.isObject()) {
                    processRow (arrayElement, appender, context);
                    context.queryBindings.clear();
                    context.allValues.clear();
                }
                // else throw Bad Json Format Exception
            }
            appender.append(']');
        } // else throw Bad Json Format Exception
    }
    
    private void processRow(JsonNode node, AkibanAppender appender, ProcessContext context) {
        
        if (context.table.getPrimaryKey() == null) {
            throw new NoSuchIndexException(Index.PRIMARY);
        }
        
        PrimaryKey pkIndex = context.table.getPrimaryKey();
        int pkFields = 0;
        Iterator<Entry<String,JsonNode>> i = node.fields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isContainerNode()) {
                throw new InvalidChildCollectionException(field.getKey());
            } else if (field.getValue().isValueNode()) {
                Column column = getColumn (context.table, field.getKey());
                if (field.getValue().isNull()) {
                    context.allValues.put(column, null);
                } else {
                    context.allValues.put (column, field.getValue().asText());
                }
                if (pkIndex.getColumns().contains(column)) {
                    pkFields++;
                    // NB: PATCH requires all PK columns to be specified. As there is no
                    // DEFAULT but they also can't actually be NULL, treat it as not present.
                    if (field.getValue().isNull()) {
                        context.allValues.remove(column);
                        i.remove();
                    }
                }
            }
        }
        
        if (pkIndex.getColumns().size() != pkFields) {
            throw new KeyColumnMissingException(pkIndex.getIndex().getIndexName().toString());
        }
        Row row = determineExistance (context);
        if (row != null) {
            runUpdate (appender, context, row);
        } else {
            appender.append(insertProcessor.processInsert(context.session, context.table.getAIS(), context.tableName, node));
        }
    }

    private Row determineExistance (ProcessContext context) {
        PlanGenerator generator = context.table.getAIS().getCachedValue(extDataService, ExternalDataServiceImpl.CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateAncestorPlan(context.table);
        Cursor cursor = null;
        try {

            Value value = new Value(context.typesTranslator.typeForString());
            int i = 0;
            for (Column column : context.table.getPrimaryKey().getColumns()) {
                // bug 1169995 - a null value in the PK won't match anything,
                // return null to force the insert. 
                if (context.allValues.get(column) == null) {
                    return null;
                }
                value.putString(context.allValues.get(column), null);
                context.queryBindings.setValue(i, value);
                i++;
            }
            cursor = API.cursor(plan, context.queryContext, context.queryBindings);
            cursor.openTopLevel();
            return cursor.next();
        } finally {
            if (cursor != null && !cursor.isClosed()) {
                cursor.close();
            }
        }
    }
    
    private void runUpdate (AkibanAppender appender, ProcessContext context, Row oldRow) {
        
        UpdateGenerator updateGenerator = getGenerator(CACHED_UPDATE_GENERATOR, context);

        List<Column> pkList = context.table.getPrimaryKey().getColumns();
        List<Column> upList = new ArrayList<>();
        Value value = new Value(context.typesTranslator.typeForString());
        int i = pkList.size();
        for (Column column : context.table.getColumns()) {
            if (!pkList.contains(column) && context.allValues.containsKey(column)) {
                if (context.allValues.get(column) == null) {
                    value.putNull();
                } else {
                    value.putString(context.allValues.get(column), null);
                }
                context.queryBindings.setValue(i, value);
                upList.add(column);
                i++;
            }
        }
        
        Operator update = updateGenerator.create(context.tableName,upList);
        Cursor cursor = API.cursor(update, context.queryContext, context.queryBindings);
        JsonRowWriter writer = new JsonRowWriter(new TableRowTracker(context.table, 0));
        WriteCapturePKRow rowWriter = new WriteCapturePKRow();
        writer.writeRows(cursor, appender, "\n", rowWriter, options);
    }
    
    private static final CacheValueGenerator<UpdateGenerator> CACHED_UPDATE_GENERATOR =
        new CacheValueGenerator<UpdateGenerator>() {
            @Override
            public UpdateGenerator valueFor(AkibanInformationSchema ais) {
                return new UpdateGenerator(ais);
            }
        };
}
