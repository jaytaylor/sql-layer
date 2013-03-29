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
import java.util.Map.Entry;

import com.akiban.server.service.externaldata.TableRowTracker;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.FKValueMismatchException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.service.externaldata.JsonRowWriter.WriteCapturePKRow;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

public class InsertProcessor extends DMLProcessor {
    private OperatorGenerator insertGenerator;
    private static final Logger LOG = LoggerFactory.getLogger(InsertProcessor.class);

    public InsertProcessor (ConfigurationService configService, 
            TreeService treeService, 
            Store store,
            T3RegistryService t3RegistryService) {
        super (configService, treeService, store, t3RegistryService);
    }
    
    private static final CacheValueGenerator<InsertGenerator> CACHED_INSERT_GENERATOR =
            new CacheValueGenerator<InsertGenerator>() {
                @Override
                public InsertGenerator valueFor(AkibanInformationSchema ais) {
                    return new InsertGenerator(ais);
                }
            };

    private class InsertContext {
        public TableName tableName;
        public UserTable table;
        public QueryContext queryContext;
        public Session session;
        public Map<Column, PValueSource> pkValues;
        public boolean anyUpdates;
        
        public InsertContext (TableName tableName, UserTable table, Session session) {
            this.table = table;
            this.tableName = tableName;
            this.session = session;
            this.queryContext = newQueryContext(session, table);
        }
    }
    
    public String processInsert(Session session, AkibanInformationSchema ais, TableName rootTable, JsonNode node) 
            {
        setAIS(ais);
        insertGenerator = getGenerator(CACHED_INSERT_GENERATOR);

        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);

        UserTable table = getTable(rootTable);
        InsertContext context = new InsertContext(rootTable, table, session);

        processContainer (node, appender, context);
        
        return appender.toString();
    }
    
    private void processContainer (JsonNode node, AkibanAppender appender, InsertContext context) {
        boolean first = true;
        Map<Column, PValueSource> pkValues = null;
        
        if (node.isObject()) {
            processTable (node, appender, context);
        } else if (node.isArray()) {
            appender.append('[');
            for (JsonNode arrayElement : node) {
                if (first) { 
                    pkValues = context.pkValues;
                    first = false;
                } else {
                    appender.append(',');
                }
                if (arrayElement.isObject()) {
                    processTable (arrayElement, appender, context);
                    context.pkValues = pkValues;
                }
                // else throw Bad Json Format Exception
            }
            appender.append(']');
        } // else throw Bad Json Format Exception
        
    }
    
    private void processTable (JsonNode node, AkibanAppender appender, InsertContext context) {
        
        // Pass one, insert fields from the table
        Iterator<Entry<String,JsonNode>> i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isValueNode()) {
                setValue (field.getKey(), field.getValue(), context);
            }
        }
        runUpdate(context, appender);
        boolean first = true;
        // pass 2: insert the child nodes
        i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isContainerNode()) {
                if (first) {
                    first = false;
                    // Delete the closing } for the object
                    StringBuilder builder = (StringBuilder)appender.getAppendable();
                    builder.deleteCharAt(builder.length()-1);
                } 
                TableName tableName = TableName.parse(context.tableName.getSchemaName(), field.getKey());
                UserTable table = getTable (tableName);
                InsertContext newContext = new InsertContext(tableName, table, context.session);
                newContext.pkValues = context.pkValues;
                appender.append(",\"");
                appender.append(table.getNameForOutput());
                appender.append("\":");
                processContainer (field.getValue(), appender, newContext);
            }
        }
        // we appended at least one sub-object, so replace the object close brace. 
        if (!first) {
            appender.append('}');
        }
    }
    
    private void setValue (String field, JsonNode node, InsertContext context) {
        Column column = getColumn (context.table, field);
        setValue (context.queryContext, column, node.asText());
    }

    private void runUpdate (InsertContext context, AkibanAppender appender) {
        assert context != null : "Bad Json format";
        LOG.trace("Insert row into: {}, values {}", context.tableName, context.queryContext);
        Operator insert = insertGenerator.create(context.table.getName());
        // If Child table, write the parent group column values into the 
        // child table join key. 
        if (context.pkValues != null && context.table.getParentJoin() != null) {
            Join join = context.table.getParentJoin();
            for (Entry<Column, PValueSource> entry : context.pkValues.entrySet()) {
                
                int pos = join.getMatchingChild(entry.getKey()).getPosition();
                PValue fkValue = getFKPvalue (entry.getValue());
                
                if (context.queryContext.getPValue(pos).isNull()) {
                    context.queryContext.setPValue(join.getMatchingChild(entry.getKey()).getPosition(), fkValue);
                } else if (TClass.compare (context.queryContext.getPValue(pos).tInstance(), 
                                context.queryContext.getPValue(pos),
                                fkValue.tInstance(),
                                fkValue) != 0) {
                    throw new FKValueMismatchException (join.getMatchingChild(entry.getKey()).getName());
                }
            }
        }
        Cursor cursor = API.cursor(insert, context.queryContext);
        JsonRowWriter writer = new JsonRowWriter(new TableRowTracker(context.table, 0));
        WriteCapturePKRow rowWriter = new WriteCapturePKRow();
        writer.writeRows(cursor, appender, context.anyUpdates ? "\n" : "", rowWriter);
        context.pkValues = rowWriter.getPKValues();
        context.anyUpdates = true;
    }
    
    private PValue getFKPvalue (PValueSource pval) {
        AkibanAppender appender = AkibanAppender.of(new StringBuilder());
        pval.tInstance().format(pval, appender);
        PValue result = new PValue(MString.varcharFor(appender.toString()), appender.toString());
        return result;
    }
    
    private void setValue (QueryContext queryContext, Column column, String value) {
        PValue pvalue = null;
        if (value == null) {
            pvalue = new PValue(Column.generateTInstance(null, Types.VARCHAR, 65535L, null, true));
            pvalue.putNull();
        } else {
            pvalue = new PValue(Column.generateTInstance(null, Types.VARCHAR, 65535L, null, true), value);
        }
        queryContext.setPValue(column.getPosition(), pvalue);
    }
    
}
