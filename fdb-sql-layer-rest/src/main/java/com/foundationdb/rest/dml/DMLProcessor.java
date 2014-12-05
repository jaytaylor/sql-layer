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

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.ValueSource;

public abstract class DMLProcessor {

    private final Store store;
    private final SchemaManager schemaManager;
    private final TypesRegistryService registryService;
    
    public DMLProcessor(Store store, SchemaManager schemaManager,
                        TypesRegistryService typesRegistryService) {
        this.store = store;
        this.schemaManager = schemaManager;
        this.registryService = typesRegistryService;
    }

    protected Column getColumn (Table table, String field) {
        Column column = table.getColumn(field);
        if (column == null) {
            throw new NoSuchColumnException(field);
        }
        return column;
    }

    protected <T extends OperatorGenerator> T getGenerator(CacheValueGenerator<T> generator, ProcessContext context) {
        T gen = context.ais().getCachedValue(this, generator);
        gen.setTypesRegistry(registryService);
        gen.setTypesTranslator(context.typesTranslator);
        return gen;
    }
    
    protected TypesTranslator getTypesTranslator() {
        return schemaManager.getTypesTranslator();
    }

    public class ProcessContext {
        public TableName tableName;
        public Table table;
        public QueryContext queryContext;
        public QueryBindings queryBindings;
        public Session session;
        public Map<Column, ValueSource> pkValues;
        public Map<Column, String> allValues;
        public boolean anyUpdates;
        private final AkibanInformationSchema ais;
        public final TypesTranslator typesTranslator;

        public ProcessContext (AkibanInformationSchema ais, Session session, TableName tableName) {
            this.tableName = tableName;
            this.ais = ais;
            this.session = session;
            this.typesTranslator = getTypesTranslator();
            this.table = getTable();
            this.queryContext = new RestQueryContext(getAdapter());
            this.queryBindings = queryContext.createBindings();
            allValues = new HashMap<>();
        }
     
        protected AkibanInformationSchema ais() {
            return ais;
        }
        
        private StoreAdapter getAdapter() {
            // no writing to the memory tables. 
            if (table.hasMemoryTableFactory())
                throw new ProtectedTableDDLException (table.getName());
            return store.createAdapter(session);
        }

        private Table getTable () {
            Table table = ais.getTable(tableName);
            if (table == null) {
                throw new NoSuchTableException(tableName.getSchemaName(), tableName.getTableName());
            } else if (table.isProtectedTable()) {
                throw  new ProtectedTableDDLException (table.getName());
            }
            return table;
        }
    }
}
