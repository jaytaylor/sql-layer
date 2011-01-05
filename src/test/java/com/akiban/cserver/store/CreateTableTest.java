package com.akiban.cserver.store;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.tree.TreeService;
import com.persistit.Exchange;
import com.persistit.Key;

public class CreateTableTest extends CServerTestCase implements CServerConstants {

    protected final static Session session = new SessionImpl();

    private final static String CREATE_TABLE_STATEMENT1 = "CREATE TABLE `coitest_1m`.`foo1` ("
            + "`a` int(11) DEFAULT NULL,"
            + "`b` int(11) DEFAULT NULL,"
            + "PRIMARY KEY (a)"
            + ") ENGINE=AKIBANDB;";

    
    @Before
    public void setUp() throws Exception {
        baseSetUp();
    }
    
    @After
    public void tearDown() throws Exception {
        baseTearDown();
    }
    
    @Test
    public void testCreateTable() throws Exception {
        schemaManager.createTableDefinition(session, "foo", CREATE_TABLE_STATEMENT1);
        final Exchange ex = serviceManager.getTreeService().getDb().getExchange(
                getDefaultVolume(), TreeService.SCHEMA_TREE_NAME,
                false);
        assertEquals(DDLSource.canonicalStatement(CREATE_TABLE_STATEMENT1), ex
                .clear().append(TreeService.BY_ID).append(1).fetch()
                .getValue().getString());
        ex.clear().append(TreeService.BY_ID).append(Key.AFTER).previous();
        assertEquals(1, ex.getKey().indexTo(-1).decodeInt());
    }

}
