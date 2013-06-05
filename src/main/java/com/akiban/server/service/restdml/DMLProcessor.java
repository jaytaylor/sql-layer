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

import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
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
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

public abstract class DMLProcessor {

    private final ConfigurationService configService;
    private final TreeService treeService;
    private final Store store;
    private final T3RegistryService registryService;
    
    public DMLProcessor (ConfigurationService configService, 
            TreeService treeService, 
            Store store,
            T3RegistryService t3RegistryService) {
        this.configService = configService;
        this.treeService = treeService;
        this.store = store;
        this.registryService = t3RegistryService;
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
            pvalue = new PValue(MString.varchar());
            pvalue.putNull();
        } else {
            pvalue = new PValue(MString.varcharFor(value), value);
        }
        queryContext.setPValue(column.getPosition(), pvalue);
        
    }

    protected OperatorGenerator getGenerator(CacheValueGenerator<? extends OperatorGenerator> generator, ProcessContext context) {
        OperatorGenerator gen = context.ais().getCachedValue(this, generator);
        gen.setT3Registry(registryService);
        return gen;
    }
    
    public class ProcessContext {
        public TableName tableName;
        public UserTable table;
        public QueryContext queryContext;
        public Session session;
        public Map<Column, PValueSource> pkValues;
        public Map<Column, String> allValues;
        public boolean anyUpdates;
        private final AkibanInformationSchema ais;
        private final Schema schema;
        
        public ProcessContext (AkibanInformationSchema ais, Session session, TableName tableName) {
            this.tableName = tableName;
            this.ais = ais;
            this.session = session;
            this.schema = SchemaCache.globalSchema(ais);
            this.table = getTable();
            this.queryContext = newQueryContext(); 
            allValues = new HashMap<>();
        }
     
        protected AkibanInformationSchema ais() {
            return ais;
        }
        
        private QueryContext newQueryContext () {
            QueryContext queryContext = new RestQueryContext(getAdapter());
            setColumnsNull (queryContext, table);
            return queryContext;
        }
        
        private StoreAdapter getAdapter() {
            // no writing to the memory tables. 
            if (table.hasMemoryTableFactory())
                throw new ProtectedTableDDLException (table.getName());
            StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
            if (adapter == null)
                adapter = store.createAdapter(session, schema);
            return adapter;
        }

        private UserTable getTable () {
            UserTable table = ais.getUserTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName.getSchemaName(), tableName.getTableName());
            } else if (table.isProtectedTable()) {
                throw  new ProtectedTableDDLException (table.getName());
            }
            return table;
        }
        protected void setColumnsNull (QueryContext queryContext, UserTable table) {
            for (Column column : table.getColumns()) {
                PValue pvalue = new PValue (column.tInstance());
                pvalue.putNull();
                queryContext.setPValue(column.getPosition(), pvalue);
            }
        }
    }
}
