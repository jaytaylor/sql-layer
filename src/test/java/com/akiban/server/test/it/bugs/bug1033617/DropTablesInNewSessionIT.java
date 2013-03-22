
package com.akiban.server.test.it.bugs.bug1033617;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public final class DropTablesInNewSessionIT extends ITBase {
    @Test
    public void test() {
        int c = createTable("schema", "customers", "cid int not null primary key, name varchar(32)");
        int o = createTable("schema", "orders", "oid int not null primary key, cid int not null, placed date",
                akibanFK("cid", "customers", "cid"));
        TableName groupName = getUserTable(c).getGroup().getName();
        createGroupIndex(groupName, "name_placed", "customers.name,orders.placed");

        writeRow(c, 1L, "bob");
        writeRow(o, 11L, 1L, "2012-01-01");

        Collection<String> indexesToUpdate = Collections.singleton("name_placed");
        ddl().updateTableStatistics(session(), TableName.create("schema", "customers"), indexesToUpdate);

        Session session = serviceManager().getSessionService().createSession();
        try {
            dropAllTables(session);
        }
        finally {
            session.close();
        }
    }
}
