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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.service.externaldata.JsonRowWriter.WritePKRow;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.util.AkibanAppender;

public class InsertProcessor {
    private final ConfigurationService configService;
    private final TreeService treeService;
    private final Store store;
    private final T3RegistryService registryService;
    private InsertGenerator insertGenerator;
    
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

    
    public String processInsert(Session session, AkibanInformationSchema ais, TableName rootTable, JsonParser jp) 
            throws JsonParseException, IOException {
        this.schema = SchemaCache.globalSchema(ais);
        
        StringBuilder write = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(write);

        insertGenerator = ais.getCachedValue(this, CACHED_INSERT_GENERATOR);
        insertGenerator.setT3Registry(registryService);
        
        //JsonGenerator generate = JsonFactory.createJsonGenerator();

        JsonToken token;

        UserTable table = ais.getUserTable(rootTable);
        String field = rootTable.getDescription();
        TableName currentTable = rootTable;

        StoreAdapter adapter;
        QueryContext queryContext = null;

        
        while((token = jp.nextToken()) != null) {
            
            switch (token) {
            case START_ARRAY:
            case START_OBJECT:
                currentTable = TableName.parse(rootTable.getSchemaName(), field);
                table = ais.getUserTable(currentTable);
                if (table == null) {
                    throw new NoSuchTableException(currentTable.getSchemaName(), currentTable.getTableName());
                }
                queryContext = newQueryContext(session, table); 
                break;
            case END_ARRAY:
            case END_OBJECT:
                runUpdate(queryContext, appender, table);
                break;
            case FIELD_NAME:
                field = jp.getCurrentName();
                break;
            case NOT_AVAILABLE:
                break;
            case VALUE_EMBEDDED_OBJECT:
                break;
            case VALUE_NULL:
                setValue(queryContext, getColumn(table, field), null);
                break;
            case VALUE_NUMBER_FLOAT:
                break;
            case VALUE_NUMBER_INT:
                setValue (queryContext, getColumn(table, field), jp.getIntValue());
                break;
            case VALUE_STRING:
                setValue (queryContext, getColumn(table, field), jp.getText());
                break;
            case VALUE_FALSE:
            case VALUE_TRUE:
                setValue (queryContext, getColumn(table, field), jp.getBooleanValue());
                break;
            default:
                break;
                
            }
        }
        
        return write.toString();
    }

    private void runUpdate (QueryContext queryContext, AkibanAppender appender, UserTable table) throws IOException {
        assert queryContext != null : "Bad Json format";
        Operator insert = insertGenerator.createInsert(table.getName());

        Cursor cursor = API.cursor(insert, queryContext);
        JsonRowWriter writer = new JsonRowWriter (table, 0);
        writer.writeRows(cursor, appender, "\n", new WritePKRow());
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
    
    
    private StoreAdapter getAdapter(Session session, UserTable table) {
        if (table.hasMemoryTableFactory())
            return new MemoryAdapter(schema, session, configService);
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = new PersistitAdapter(schema, store, treeService, session, configService);
        return adapter;
    }
    
    private QueryContext newQueryContext (Session session, UserTable table) {
        QueryContext queryContext = new SimpleQueryContext(getAdapter(session, table));
        
        for (Column column : table.getColumns()) {
            PValue pvalue = new PValue (column.tInstance());
            pvalue.putNull();
            queryContext.setPValue(column.getPosition(), pvalue);
        }
        
        return queryContext;

    }
}
