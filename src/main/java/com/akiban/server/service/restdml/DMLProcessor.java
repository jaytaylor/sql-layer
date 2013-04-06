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
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
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
                   TableName.SECURITY_SCHEMA.equals(schemaName) ||
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

    protected void setValue (QueryContext queryContext, Column column, String value) {
        PValue pvalue = null;
        if (value == null) {
            pvalue = new PValue(Column.generateTInstance(null, Types.VARCHAR, 65535L, null, true));
            pvalue.putNull();
        } else {
            pvalue = new PValue(Column.generateTInstance(null, Types.VARCHAR, 65535L, null, true), value);
        }
        queryContext.setPValue(column.getPosition(), pvalue);
    }

    protected OperatorGenerator getGenerator(CacheValueGenerator<? extends OperatorGenerator> generator) {
        OperatorGenerator gen = ais.getCachedValue(this, generator);
        gen.setT3Registry(registryService);
        return gen;
    }
}
