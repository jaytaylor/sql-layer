package com.akiban.cserver.store;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.persistit.Exchange;
import com.persistit.Key;

public class CreateTableTest extends CServerTestCase implements CServerConstants {

    protected final static Session session = new SessionImpl();

    private final static String CREATE_TABLE_STATEMENT1 = "CREATE TABLE `coitest_1m`.`foo1` ("
            + "`a` int(11) DEFAULT NULL,"
            + "`b` int(11) DEFAULT NULL,"
            + "PRIMARY KEY (a)"
            + ") ENGINE=AKIBANDB;";

    public void testCreateTable() throws Exception {
        schemaManager.createTableDefinition(session, "foo", CREATE_TABLE_STATEMENT1);
        final Exchange ex = serviceManager.getPersistitService().getDb().getExchange(
                PersistitService.VOLUME_NAME, PersistitService.SCHEMA_TREE_NAME,
                false);
        assertEquals(DDLSource.canonicalStatement(CREATE_TABLE_STATEMENT1), ex
                .clear().append(PersistitService.BY_ID).append(1).fetch()
                .getValue().getString());
        ex.clear().append(PersistitService.BY_ID).append(Key.AFTER).previous();
        assertEquals(1, ex.getKey().indexTo(-1).decodeInt());
    }

}
