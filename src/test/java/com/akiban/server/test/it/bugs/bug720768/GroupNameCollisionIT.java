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

package com.akiban.server.test.it.bugs.bug720768;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.staticgrouping.GroupsBuilder;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GroupNameCollisionIT extends ITBase {
    @Test
    public void tablesWithSameNames() {

        try {
            createTable("s1", "t", "id int not null primary key");
            createTable("s2", "t", "id int not null primary key");
            createTable("s1", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
            createTable("s2", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        AkibanInformationSchema ais = ddl().getAIS(session());
        final Group group1 = ais.getUserTable("s1", "t").getGroup();
        final Group group2 = ais.getUserTable("s2", "t").getGroup();
        if (group1.getName().equals(group2.getName())) {
            fail("same group names: " + group1 + " and " + group2);
        }

        GroupsBuilder expectedBuilder = new GroupsBuilder("foo");
        for(com.akiban.ais.model.Group aisGroup : ais.getGroups().values()) {
            TableName rootTable = aisGroup.getRoot().getName();
            if (TableName.INFORMATION_SCHEMA.equals(rootTable.getSchemaName())) {
                expectedBuilder.rootTable(rootTable, aisGroup.getName());
                if ("index_statistics".equals(rootTable.getTableName())) {
                  expectedBuilder.joinTables(rootTable,
                                             new TableName(rootTable.getSchemaName(),
                                                           "index_statistics_entry"))
                    .column("table_id", "table_id")
                    .column("index_id", "index_id");
                }
            }
        }
        expectedBuilder.rootTable("s1", "t", group1.getName());
        expectedBuilder.joinTables("s1", "t", "s1", "c").column("id", "pid");
        expectedBuilder.rootTable("s2", "t", group2.getName());
        expectedBuilder.joinTables("s2", "t", "s2", "c").column("id", "pid");

        assertEquals("grouping",
                expectedBuilder.getGrouping().toString(),
                GroupsBuilder.fromAis(ais, "foo").toString()
        );
    }
}
