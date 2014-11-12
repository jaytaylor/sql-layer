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

package com.foundationdb.server.test.it.memorytable;

import static org.junit.Assert.*;

import com.foundationdb.qp.memoryadapter.MemoryGroupCursor;
import com.foundationdb.qp.util.SchemaCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.sql.ServerSessionITBase;

public class MemoryAdapterIT extends ServerSessionITBase {
    private static final TableName TEST_NAME = new TableName (TableName.INFORMATION_SCHEMA, "test");
    private SchemaManager schemaManager;
    private Table table;
    
    private void registerISTable(final Table table, final MemoryTableFactory factory) throws Exception {
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }

    @Before
    public void setUp() throws Exception {
        schemaManager = serviceManager().getSchemaManager();
    }

    @After
    public void unRegister() {
        if(ais().getTable(TEST_NAME) != null) {
            schemaManager.unRegisterMemoryInformationSchemaTable(TEST_NAME);
        }
    }

    @Test
    public void insertFactoryTest() throws Exception {
  
        table = AISBBasedBuilder.create(ddl().getTypesTranslator()).table(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colInt("c1").pk("c1").ais().getTable(TEST_NAME);
        MemoryTableFactory factory = new TestFactory (TEST_NAME);

        registerISTable(table, factory);
 
        Table table = ais().getTable(TEST_NAME);
        assertNotNull (table);
        assertTrue (table.hasMemoryTableFactory());
    }

    @Test
    public void testGetAdapter() throws Exception {
        table = AISBBasedBuilder.create(ddl().getTypesTranslator()).table(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colInt("c1").pk("c1").ais().getTable(TEST_NAME);
        MemoryTableFactory factory = new TestFactory (TEST_NAME);
        registerISTable(table, factory);
        Table newtable = ais().getTable(TEST_NAME);
        
        TestSession sqlSession = new TestSession();

        StoreAdapter adapter = sqlSession.getStoreHolder(SchemaCache.globalSchema(ais())).getAdapter(newtable);
        assertNotNull (adapter);
        assertTrue (adapter instanceof MemoryAdapter);
    }

    private class TestFactory implements MemoryTableFactory {
        
        private TableName name;
        public TestFactory (TableName name) {
            this.name = name;
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adaper) {
            return null;
        }

        @Override
        public TableName getName() {
            return name;
        }

        @Override
        public long rowCount(Session session) {
            return 0;
        }
    }
}
