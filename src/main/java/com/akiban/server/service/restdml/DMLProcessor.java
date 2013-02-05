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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ProtectedTableDDLException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.pvalue.PValue;

public abstract class DMLProcessor {

    private final ConfigurationService configService;
    private final TreeService treeService;
    private final Store store;
    private final T3RegistryService registryService;
    private Schema schema;
    private AkibanInformationSchema ais;
    
    public DMLProcessor (ConfigurationService configService, 
            TreeService treeService, 
            Store store,
            T3RegistryService t3RegistryService) {
        this.configService = configService;
        this.treeService = treeService;
        this.store = store;
        this.registryService = t3RegistryService;
    }

    protected void setAIS (AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = SchemaCache.globalSchema(ais);
    }
    
    protected QueryContext newQueryContext (Session session, UserTable table) {
        QueryContext queryContext = new RestQueryContext(getAdapter(session, table));
        setColumnsNull (queryContext, table);
        return queryContext;
    }
    
    protected StoreAdapter getAdapter(Session session, UserTable table) {
        // no writing to the memory tables. 
        if (table.hasMemoryTableFactory())
            throw new ProtectedTableDDLException (table.getName());
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = new PersistitAdapter(schema, store, treeService, session, configService);
        return adapter;
    }

    protected void setColumnsNull (QueryContext queryContext, UserTable table) {
        for (Column column : table.getColumns()) {
            PValue pvalue = new PValue (column.tInstance());
            pvalue.putNull();
            queryContext.setPValue(column.getPosition(), pvalue);
        }
    }
    
    protected UserTable getTable (TableName name) {
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
    
    protected Column getColumn (UserTable table, String field) {
        Column column = table.getColumn(field);
        if (column == null) {
            throw new NoSuchColumnException(field);
        }
        return column;
    }
    
    protected OperatorGenerator getGenerator(CacheValueGenerator<? extends OperatorGenerator> generator) {
        OperatorGenerator gen = ais.getCachedValue(this, generator);
        gen.setT3Registry(registryService);
        return gen;
    }
}
