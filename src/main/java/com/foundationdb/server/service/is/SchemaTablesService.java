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

package com.foundationdb.server.service.is;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.store.SchemaManager;

public class SchemaTablesService {
    
    static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
    public static final int YES_NO_MAX = 3;
    public static final int DESCRIPTOR_MAX = 32;
    public static final int IDENT_MAX = 128;
    public static final int PATH_MAX = 1024;
    protected final SchemaManager schemaManager;

    public SchemaTablesService (SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
    
    protected void attach(AkibanInformationSchema ais, TableName name, Class<? extends BasicFactoryBase> clazz) {
        Table table = ais.getTable(name);
        assert table != null;
        final BasicFactoryBase factory;
        try {
            factory = clazz.getConstructor(this.getClass(), TableName.class).newInstance(this, name);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }
    
    protected void attach (AkibanInformationSchema ais, TableName name, BasicFactoryBase factory) {
        Table table = ais.getTable(name);
        assert table != null;
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }
    
    protected void attach (Table table, BasicFactoryBase factory) {
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }
    
    protected abstract class BaseScan implements GroupScan {
        final RowType rowType;
        long rowCounter = 0;
        
        public BaseScan (RowType rowType) {
            this.rowType = rowType;
        }
        
        @Override
        public void close() {
        }
        
    } 
}
