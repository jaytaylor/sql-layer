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
package com.foundationdb.server.service.restdml;

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.t3expressions.T3RegistryService;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;

public abstract class DMLProcessor {

    private final Store store;
    private final T3RegistryService registryService;
    
    public DMLProcessor(Store store,
                        T3RegistryService t3RegistryService) {
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

    protected void setValue (QueryBindings queryBindings, Column column, String value) {
        PValue pvalue = null;
        if (value == null) {
            pvalue = new PValue(MString.varchar());
            pvalue.putNull();
        } else {
            pvalue = new PValue(MString.varcharFor(value), value);
        }
        queryBindings.setPValue(column.getPosition(), pvalue);
        
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
        public QueryBindings queryBindings;
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
            this.queryContext = new RestQueryContext(getAdapter());
            this.queryBindings = queryContext.createBindings();
            allValues = new HashMap<>();
            setColumnsNull (queryBindings, table);
        }
     
        protected AkibanInformationSchema ais() {
            return ais;
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
        protected void setColumnsNull (QueryBindings queryBindings, UserTable table) {
            for (Column column : table.getColumns()) {
                PValue pvalue = new PValue (column.tInstance());
                pvalue.putNull();
                queryBindings.setPValue(column.getPosition(), pvalue);
            }
        }
    }
}
