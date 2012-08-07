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

package com.akiban.server.test.it.bugs.bug1033617;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public final class ThisTestNeedsABetterNameIT extends ITBase {
    @Test
    public void test() {
        int c = createTable("schema", "customers", "cid int not null primary key, name varchar(32)");
        int o = createTable("schema", "orders", "oid int not null primary key, cid int not null, placed date",
                akibanFK("cid", "customers", "cid"));
        String groupName = getUserTable(c).getGroup().getName();
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
