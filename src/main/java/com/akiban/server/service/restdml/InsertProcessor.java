/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.server.service.restdml;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
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
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
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
        
        public InsertContext (TableName tableName, UserTable table, Session session) {
            this.table = table;
            this.tableName = tableName;
            this.session = session;
            this.queryContext = newQueryContext(session, table);
        }
    }
    
    public String processInsert(Session session, AkibanInformationSchema ais, TableName rootTable, JsonNode node) 
            throws JsonParseException, IOException {
        setAIS(ais);
        insertGenerator = getGenerator(CACHED_INSERT_GENERATOR);

        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);

        UserTable table = getTable(rootTable);
        InsertContext context = new InsertContext(rootTable, table, session);

        processContainer (node, appender, context);
        
        return appender.toString();
    }
    
    private void processContainer (JsonNode node, AkibanAppender appender, InsertContext context) throws IOException {
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
    
    private void processTable (JsonNode node, AkibanAppender appender, InsertContext context) throws IOException {
        
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
                appender.append(tableName.getDescription());
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
        
        if (node.isBoolean()) {
            setValue (context.queryContext, column, node.asBoolean());
        } else if (node.isInt()) {
            setValue (context.queryContext, column, node.asInt());
        } else if (node.isLong()) {
            setValue (context.queryContext, column, node.asLong());
        } else if (node.isDouble()) {
            setValue (context.queryContext, column, node.asDouble());
        } else if (node.isTextual() || node.isNull()) {
            // Also handles NULLs 
            setValue (context.queryContext, column, node.asText());
        }
        
    }

    private void runUpdate (InsertContext context, AkibanAppender appender) throws IOException { 
        assert context != null : "Bad Json format";
        LOG.trace("Insert row into: {}", context.tableName);
        Operator insert = insertGenerator.create(context.table.getName());
        // If Child table, write the parent group column values into the 
        // child table join key. 
        if (context.pkValues != null && context.table.getParentJoin() != null) {
            Join join = context.table.getParentJoin();
            for (Entry<Column, PValueSource> entry : context.pkValues.entrySet()) {
                
                int pos = join.getMatchingChild(entry.getKey()).getPosition();
                
                if (context.queryContext.getPValue(pos).isNull()) {
                    context.queryContext.setPValue(join.getMatchingChild(entry.getKey()).getPosition(), entry.getValue());
                } else if (TClass.compare (context.queryContext.getPValue(pos).tInstance(), 
                                context.queryContext.getPValue(pos),
                                entry.getValue().tInstance(),
                                entry.getValue()) != 0) {
                    throw new FKValueMismatchException (join.getMatchingChild(entry.getKey()).getName());
                }
            }
        }
        Cursor cursor = API.cursor(insert, context.queryContext);
        JsonRowWriter writer = new JsonRowWriter (context.table, 0);
        WriteCapturePKRow rowWriter = new WriteCapturePKRow();
        writer.writeRows(cursor, appender, "\n", rowWriter);
        context.pkValues = rowWriter.getPKValues();
    }
    
    private void setValue (QueryContext queryContext, Column column, String value) {
        PValue pvalue = null;
        if (value == null) {
            pvalue = new PValue(column.tInstance());
            pvalue.putNull();
        } else if (column.getType().equals(Types.DATETIME)) {
            pvalue = new PValue(column.tInstance(), MDatetimes.parseDatetime(value));
        } else if (column.getType().equals(Types.DATE)) {
            int date = MDatetimes.parseDate(value, new TExecutionContext(null, null, queryContext));
            pvalue = new PValue(column.tInstance(), date);
        } else if (column.getType().equals(Types.TIME)) {
            int time = MDatetimes.parseTime(value,  new TExecutionContext(null, null, queryContext));
            pvalue = new PValue(column.tInstance(), time);
        } else {
            pvalue = new PValue(column.tInstance(), value);
        }
        queryContext.setPValue(column.getPosition(), pvalue);
    }
    
    private void setValue (QueryContext queryContext,Column column, int value) {
        queryContext.setPValue(column.getPosition(), new PValue (column.tInstance(), value));
    }
    
    private void setValue (QueryContext queryContext, Column column, long value) {
        queryContext.setPValue(column.getPosition(),  new PValue (column.tInstance(), value));
    }
    
    private void setValue (QueryContext queryContext, Column column, boolean value) {
        queryContext.setPValue(column.getPosition(), new PValue(column.tInstance(), value));
    }
    
    private void setValue (QueryContext queryContext, Column column, double value) {
        queryContext.setPValue(column.getPosition(), new PValue(column.tInstance(), value));
    }
}
