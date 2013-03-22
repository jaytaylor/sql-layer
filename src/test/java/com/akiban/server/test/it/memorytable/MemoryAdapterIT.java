
package com.akiban.server.test.it.memorytable;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.Callable;

import com.akiban.qp.memoryadapter.MemoryGroupCursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.sql.ServerSessionITBase;

public class MemoryAdapterIT extends ServerSessionITBase {
    private static final TableName TEST_NAME = new TableName (TableName.INFORMATION_SCHEMA, "test");
    private SchemaManager schemaManager;
    private UserTable table;
    
    private void registerISTable(final UserTable table, final MemoryTableFactory factory) throws Exception {
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }

    @Before
    public void setUp() throws Exception {
        schemaManager = serviceManager().getSchemaManager();
    }

    @After
    public void unRegister() {
        if(ais().getUserTable(TEST_NAME) != null) {
            schemaManager.unRegisterMemoryInformationSchemaTable(TEST_NAME);
        }
    }

    @Test
    public void insertFactoryTest() throws Exception {
  
        table = AISBBasedBuilder.create().userTable(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colLong("c1").pk("c1").ais().getUserTable(TEST_NAME);
        MemoryTableFactory factory = new TestFactory (TEST_NAME);

        registerISTable(table, factory);
 
        UserTable table = ais().getUserTable(TEST_NAME);
        assertNotNull (table);
        assertTrue (table.hasMemoryTableFactory());
    }

    @Test
    public void testGetAdapter() throws Exception {
        table = AISBBasedBuilder.create().userTable(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colLong("c1").pk("c1").ais().getUserTable(TEST_NAME);
        MemoryTableFactory factory = new TestFactory (TEST_NAME);
        registerISTable(table, factory);
        UserTable newtable = ais().getUserTable(TEST_NAME);
        
        TestSession sqlSession = new TestSession();

        StoreAdapter adapter = sqlSession.getStore(newtable);
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
        public Cursor getIndexCursor(Index index, Session session,
                IndexKeyRange keyRange, Ordering ordering,
                IndexScanSelector scanSelector) {
            return null;
        }

        @Override
        public TableName getName() {
            return name;
        }

        @Override
        public long rowCount() {
            return 0;
        }

        @Override
        public IndexStatistics computeIndexStatistics(Session session, Index index) {
            return null;
        }
    }
}
