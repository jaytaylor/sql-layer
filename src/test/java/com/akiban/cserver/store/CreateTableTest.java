package com.akiban.cserver.store;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.RowDefCache;
import com.persistit.Exchange;
import com.persistit.Key;

public class CreateTableTest extends TestCase implements CServerConstants {

    private final static String CREATE_TABLE_STATEMENT1 = "CREATE TABLE `coitest_1m`.`foo1` ("
            + "`a` int(11) DEFAULT NULL,"
            + "`b` int(11) DEFAULT NULL,"
            + "PRIMARY KEY (a)"
            + ") ENGINE=AKIBADB;";

    private PersistitStore store;

    private RowDefCache rowDefCache;

    @Override
    public void setUp() throws Exception {
        rowDefCache = new RowDefCache();
        store = new PersistitStore(CServerConfig.unitTestConfig(), rowDefCache);
        store.startUp();
        store.setOrdinals();
    }

    @Override
    public void tearDown() throws Exception {
        store.shutDown();
        store = null;
        rowDefCache = null;
    }

    public void testCreateTable() throws Exception {
        assertEquals("result", CServerConstants.OK, store.createTable("foo", CREATE_TABLE_STATEMENT1));
        final Exchange ex = store.getDb().getExchange(
                PersistitStore.VOLUME_NAME, PersistitStore.SCHEMA_TREE_NAME,
                false);
        assertEquals(DDLSource.canonicalStatement(CREATE_TABLE_STATEMENT1), ex
                .clear().append(PersistitStore.BY_ID).append(1).fetch()
                .getValue().getString());
        ex.clear().append(PersistitStore.BY_ID).append(Key.AFTER).previous();
        assertEquals(1, ex.getKey().indexTo(-1).decodeInt());
    }

}
