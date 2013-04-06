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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.externaldata.PlanGenerator;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.util.AkibanAppender;

public class UpsertProcessor extends DMLProcessor {

    private final InsertProcessor insertProcessor;
    private OperatorGenerator updateGenerator;

    
    public UpsertProcessor(ConfigurationService configService,
            TreeService treeService, Store store,
            T3RegistryService t3RegistryService,
            InsertProcessor insertProcessor) {
        super(configService, treeService, store, t3RegistryService);
        this.insertProcessor = insertProcessor;
    }

    
    public String processUpsert(Session session, AkibanInformationSchema ais, TableName tableName, JsonNode node) {
        setAIS(ais);

        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);

        UserTable table = getTable(tableName);
        UpdateContext context = new UpdateContext(tableName, table, session);

        processContainer (node, appender, context);
        
        return appender.toString();

    }
    
    private void processContainer (JsonNode node, AkibanAppender appender, UpdateContext context) {
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
                }
                // else throw Bad Json Format Exception
            }
            appender.append(']');
        } // else throw Bad Json Format Exception
    }
    
    private void processRow(JsonNode node, AkibanAppender appender, UpdateContext context) {
        
        if (context.table.getPrimaryKey() == null) {
            throw new NoSuchIndexException(Index.PRIMARY_KEY_CONSTRAINT);
        }
        
        TableIndex pkIndex = context.table.getPrimaryKey().getIndex();
        
        Iterator<Entry<String,JsonNode>> i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field =i.next();
            if (field.getValue().isValueNode()) {
                Column column = getColumn (context.table, field.getKey());
                context.allValues.put (column, field.getValue().asText());
                if (pkIndex.getKeyColumns().contains(column)) {
                    context.pkValues.put(column, field.getValue().asText());
                }
            }
        }
        
        if (pkIndex.getKeyColumns().size() != context.pkValues.size()) {
            //TODO Write New error - code and message -> JsonNode object needs all PK field with values
            throw new RuntimeException();
        }
        
        
        
        Row row = determineExistance (context);
        if (row != null) {
            runUpdate (appender, context, row);
        } else {
            appender.append(insertProcessor.processInsert(context.session, context.table.getAIS(), context.tableName, node));
        }
    }

    private Row determineExistance (UpdateContext context) {
        PlanGenerator generator = context.table.getAIS().getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateAncestorPlan(context.table);
        Cursor cursor = null;
        try {
            cursor = API.cursor(plan, context.queryContext);

            PValue pvalue = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
            int i = 0;
            for (String value: context.pkValues.values()) {
                pvalue.putString(value, null);
                context.queryContext.setPValue(i, pvalue);
                i++;
            }
            return cursor.next();
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }
    
    private void runUpdate (AkibanAppender appender, UpdateContext context, Row oldRow) {
        updateGenerator = getGenerator(CACHED_UPDATE_GENERATOR);
        Operator update =  updateGenerator.get(context.tableName);
        
        
        
    }
    
    private static final CacheValueGenerator<UpdateGenerator> CACHED_UPDATE_GENERATOR =
        new CacheValueGenerator<UpdateGenerator>() {
            @Override
            public UpdateGenerator valueFor(AkibanInformationSchema ais) {
                return new UpdateGenerator(ais);
            }
        };

    private static final CacheValueGenerator<PlanGenerator> CACHED_PLAN_GENERATOR =
            new CacheValueGenerator<PlanGenerator>() {
                @Override
                public PlanGenerator valueFor(AkibanInformationSchema ais) {
                    return new PlanGenerator(ais);
                }
            };
        
    private class UpdateContext {
        public TableName tableName;
        public UserTable table;
        public QueryContext queryContext;
        public Session session;
        public Map<Column, String> pkValues;
        public Map<Column, String> allValues;
        
        public UpdateContext (TableName tableName, UserTable table, Session session) {
            this.table = table;
            this.tableName = tableName;
            this.session = session;
            this.queryContext = newQueryContext(session, table);
            pkValues = new TreeMap<>();
            allValues = new TreeMap<>();
        }
    }

}
