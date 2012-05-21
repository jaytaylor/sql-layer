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

import org.junit.Test;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryStore;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.service.session.Session;
import com.akiban.sql.pg.PostgresServerConnection;
import com.akiban.sql.pg.PostgresServerITBase;

public class MemoryAdapterIT extends PostgresServerITBase {

    @Test
    public void getAdapterTest() {
        MemoryStore store = serviceManager().getMemoryStore();
        
        assertNotNull (store);
        
    }
    
    @Test
    public void insertFactoryTest() {
        MemoryStore store = serviceManager().getMemoryStore();
        
        TableName name = new TableName ("test", "test");
        store.registerTable(name, new TestFactory(name));
        
        MemoryTableFactory factory = store.getFactory(name);
        assertNotNull(factory);
        assertEquals (factory.getName(), name);
    }
    
    @Test
    public void testGetAdapter() throws Exception {

        assertNotNull (connection);
        Statement executeStatement = connection.createStatement();
        executeStatement.execute("select 1");
        
        Set<Integer> connections =  serviceManager().getPostgresService().getServer().getCurrentSessions();
        assertTrue (!connections.isEmpty());
        Integer first = connections.iterator().next();
        assertNotNull (first);
        
        PostgresServerConnection postgresConn = serviceManager().getPostgresService().getServer().getConnection(first.intValue());
        assertNotNull(postgresConn);
       
        MemoryStore store = serviceManager().getMemoryStore();
        
        TableName name = new TableName ("test", "test");
        store.registerTable(name, new TestFactory(name));
        
        StoreAdapter adapter = postgresConn.getStore(name);
        assertNotNull (adapter);
        assertTrue (adapter instanceof MemoryAdapter);

        TableName newName = new TableName ("test", "foo");
        adapter = postgresConn.getStore(newName);
        assertNotNull (adapter);
        assertTrue (adapter instanceof PersistitAdapter);
    }
    
    private class TestFactory implements MemoryTableFactory {
        public TestFactory (TableName name) {
            this.name = name;
        }
        @Override
        public GroupCursor getGroupCursor(Session session) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Cursor getIndexCursor(Index index, Session session,
                IndexKeyRange keyRange, Ordering ordering,
                IndexScanSelector scanSelector) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TableName getName() {
            return name;
        }

        @Override
        public Table getTableDefinition() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public long rowCount() {
            // TODO Auto-generated method stub
            return 0;
        }
        private TableName name;
    }
}
