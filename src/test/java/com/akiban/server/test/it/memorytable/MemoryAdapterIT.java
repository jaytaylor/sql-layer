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
import com.akiban.sql.pg.PostgresServerConnection;
import com.akiban.sql.pg.PostgresServerITBase;

public class MemoryAdapterIT extends PostgresServerITBase {
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
        Connection connection = openConnection();
        Statement executeStatement = connection.createStatement();
        executeStatement.execute("select 1");

        Set<Integer> connections =  serviceManager().getPostgresService().getServer().getCurrentSessions();
        assertTrue (!connections.isEmpty());
        Integer first = connections.iterator().next();
        assertNotNull (first);

        PostgresServerConnection postgresConn = serviceManager().getPostgresService().getServer().getConnection(first.intValue());
        assertNotNull(postgresConn);
        
        table = AISBBasedBuilder.create().userTable(TEST_NAME.getSchemaName(),TEST_NAME.getTableName()).colLong("c1").pk("c1").ais().getUserTable(TEST_NAME);
        MemoryTableFactory factory = new TestFactory (TEST_NAME);
        registerISTable(table, factory);
        UserTable newtable = ais().getUserTable(TEST_NAME);
        
        StoreAdapter adapter = postgresConn.getStore(newtable);
        assertNotNull (adapter);
        assertTrue (adapter instanceof MemoryAdapter);

        closeConnection(connection);
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
