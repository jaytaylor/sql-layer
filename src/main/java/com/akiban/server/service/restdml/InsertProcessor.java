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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
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
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.FKValueMismatchException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ProtectedTableDDLException;
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

public class InsertProcessor {
    private final ConfigurationService configService;
    private final TreeService treeService;
    private final Store store;
    private final T3RegistryService registryService;
    private InsertGenerator insertGenerator;
    private static final Logger LOG = LoggerFactory.getLogger(InsertProcessor.class);

    private Schema schema;
    
    public InsertProcessor (ConfigurationService configService, 
            TreeService treeService, 
            Store store,
            T3RegistryService t3RegistryService) {
        this.configService = configService;
        this.treeService = treeService;
        this.store = store;
        this.registryService = t3RegistryService;
    }
    
    private static final CacheValueGenerator<InsertGenerator> CACHED_INSERT_GENERATOR =
            new CacheValueGenerator<InsertGenerator>() {
                @Override
                public InsertGenerator valueFor(AkibanInformationSchema ais) {
                    return new InsertGenerator(ais);
                }
            };

    private enum JsonStatus { START, DONE, OBJECT, ARRAY_FIRST, ARRAY_SECOND, ROOT_FIRST, ROOT_SECOND } 
    
    private class InsertContext {
        public TableName tableName;
        public UserTable table;
        public QueryContext queryContext;
        public JsonStatus status; 
        public boolean inObject;
        public Map<Column, PValueSource> pkValues;
        
        public InsertContext (TableName tableName, UserTable table, QueryContext queryContext) {
            this.table = table;
            this.tableName = tableName;
            this.queryContext = queryContext;
            status = JsonStatus.START;
            inObject = false;
        }
    }
    
    public String processInsert(Session session, AkibanInformationSchema ais, TableName rootTable, JsonParser jp) 
            throws JsonParseException, IOException {
        this.schema = SchemaCache.globalSchema(ais);
        Stack<InsertContext> insertContext  = new Stack<>();
        StringBuilder builder = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(builder);

        insertGenerator = ais.getCachedValue(this, CACHED_INSERT_GENERATOR);
        insertGenerator.setT3Registry(registryService);
        
        //JsonGenerator generate = JsonFactory.createJsonGenerator();

        JsonToken token;
        UserTable table = getTable(ais, rootTable);
        String field = rootTable.getDescription();
        TableName currentTable = rootTable;
        insertContext.push(new InsertContext(rootTable, table, newQueryContext(session, table)));
        
        while((token = jp.nextToken()) != null) {
            
            switch (token) {
            case START_ARRAY:
                if (insertContext.peek().inObject) {
                    LOG.debug("Starting sub table: {}", field);
                    if (insertContext.peek().status == JsonStatus.ARRAY_SECOND) {
                        appender.append(',');
                    }
                    if (insertContext.peek().status == JsonStatus.ARRAY_FIRST ||
                        insertContext.peek().status == JsonStatus.ARRAY_SECOND) {
                        insertContext.peek().pkValues = (insertContext.size() > 1) ? 
                                insertContext.get(insertContext.size()-2).pkValues :
                                null;
                        runUpdate(insertContext.peek(), appender);
                        // delete the closing } for the object
                        builder.deleteCharAt(builder.length()-1);
                        insertContext.peek().status = JsonStatus.ROOT_FIRST;
                    }
                    currentTable = TableName.parse(rootTable.getSchemaName(), field);
                    table = getTable(ais, currentTable);
                    insertContext.push(new InsertContext(currentTable, table, newQueryContext(session, table)));
                    appender.append(",\"");
                    appender.append(currentTable.getDescription());
                    appender.append("\":");
                }
                appender.append('[');
                insertContext.peek().status = JsonStatus.ARRAY_FIRST;
                break;
            case START_OBJECT:
                if (insertContext.peek().status == JsonStatus.ROOT_FIRST) {
                    appender.append(',');
                    setColumnsNull(insertContext.peek().queryContext, insertContext.peek().table);
                    insertContext.peek().status = JsonStatus.ARRAY_FIRST;
                } else if (insertContext.peek().status == JsonStatus.ARRAY_SECOND) {
                    setColumnsNull(insertContext.peek().queryContext, insertContext.peek().table);
                }
                insertContext.peek().inObject = true;
                break;
            case END_ARRAY:
                appender.append(']');
                insertContext.pop();
                break;
            case END_OBJECT:
                if (insertContext.peek().status == JsonStatus.ARRAY_FIRST) {
                    insertContext.peek().status = JsonStatus.ARRAY_SECOND;
                } else if (insertContext.peek().status == JsonStatus.ARRAY_SECOND) {
                    appender.append(','); 
                } else if (insertContext.peek().status == JsonStatus.ROOT_FIRST) {
                    appender.append('}');
                    insertContext.peek().inObject = false;
                } 
                if (insertContext.peek().inObject) {
                    insertContext.peek().pkValues = (insertContext.size() > 1) ? 
                            insertContext.get(insertContext.size()-2).pkValues :
                            null;
                    runUpdate(insertContext.peek(), appender); 
                    insertContext.peek().inObject = false;
                }
                break;
            case FIELD_NAME:
                field = jp.getCurrentName();
                break;
            case NOT_AVAILABLE:
                break;
            case VALUE_EMBEDDED_OBJECT:
                break;
            case VALUE_NULL:
                setValue(insertContext.peek().queryContext, 
                        getColumn(insertContext.peek().table, field),
                        null);
                break;
            case VALUE_NUMBER_FLOAT:
                setValue (insertContext.peek().queryContext, 
                        getColumn(insertContext.peek().table, field), 
                        jp.getFloatValue());
                break;
            case VALUE_NUMBER_INT:
                setValue (insertContext.peek().queryContext, 
                        getColumn(insertContext.peek().table, field), 
                        jp.getIntValue());
                break;
            case VALUE_STRING:
                setValue (insertContext.peek().queryContext, 
                        getColumn(insertContext.peek().table, field), 
                        jp.getText());
                break;
            case VALUE_FALSE:
            case VALUE_TRUE:
                setValue (insertContext.peek().queryContext, 
                        getColumn(insertContext.peek().table, field), 
                        jp.getBooleanValue());
                break;
            default:
                break;
                
            }
        }
        LOG.debug(appender.toString());
        return appender.toString();
    }

    private void runUpdate (InsertContext context, AkibanAppender appender) throws IOException { 
        assert context != null : "Bad Json format";
        Operator insert = insertGenerator.createInsert(context.table.getName());
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
    
    private Column getColumn (UserTable table, String field) {
        Column column = table.getColumn(field);
        if (column == null) {
            throw new NoSuchColumnException(field);
        }
        return column;
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
    
    private void setValue (QueryContext queryContext, Column column, boolean value) {
        queryContext.setPValue(column.getPosition(), new PValue(column.tInstance(), value));
    }
    
    private void setValue (QueryContext queryContext, Column column, float value) {
        queryContext.setPValue(column.getPosition(), new PValue(column.tInstance(), value));
    }
    
    private StoreAdapter getAdapter(Session session, UserTable table) {
        // no writing to the memory tables. 
        if (table.hasMemoryTableFactory())
            throw new ProtectedTableDDLException (table.getName());
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = new PersistitAdapter(schema, store, treeService, session, configService);
        return adapter;
    }
    
    private QueryContext newQueryContext (Session session, UserTable table) {
        QueryContext queryContext = new RestQueryContext(getAdapter(session, table));
        setColumnsNull (queryContext, table);
        return queryContext;
    }
    
    private void setColumnsNull (QueryContext queryContext, UserTable table) {
        for (Column column : table.getColumns()) {
            PValue pvalue = new PValue (column.tInstance());
            pvalue.putNull();
            queryContext.setPValue(column.getPosition(), pvalue);
        }
    }
    
    private UserTable getTable (AkibanInformationSchema ais, TableName name) {
        UserTable table = ais.getUserTable(name);
        String schemaName = name.getSchemaName();
        if (table == null) {
            throw new NoSuchTableException(name.getSchemaName(), name.getTableName());
        } else if (TableName.INFORMATION_SCHEMA.equals(schemaName) ||
                    TableName.SYS_SCHEMA.equals(schemaName) ||
                    TableName.SQLJ_SCHEMA.equals(schemaName)) {
            throw  new ProtectedTableDDLException (table.getName());
        }
        return table;
    }
}
