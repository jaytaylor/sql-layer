/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
            createTable("s1", "t", "id int key");
            createTable("s2", "t", "id int key");
            createTable("s1", "c", "id int key, pid int",
                    "CONSTRAINT __akiban_fk1 FOREIGN KEY __akiban_fk1(pid) REFERENCES t(id)");
            createTable("s2", "c", "id int key, pid int",
                    "CONSTRAINT __akiban_fk1 FOREIGN KEY __akiban_fk1(pid) REFERENCES t(id)");
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
            TableName rootTable = aisGroup.getGroupTable().getRoot().getName();
            if ("akiban_information_schema".equals(rootTable.getSchemaName())) {
                expectedBuilder.rootTable(rootTable, aisGroup.getName());
                if ("zindex_statistics".equals(rootTable.getTableName())) {
                  expectedBuilder.joinTables(rootTable,
                                             new TableName(rootTable.getSchemaName(),
                                                           "zindex_statistics_entry"))
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
