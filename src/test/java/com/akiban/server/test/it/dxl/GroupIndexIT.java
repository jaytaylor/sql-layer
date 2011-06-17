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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GroupIndexIT extends ITBase {
    private void createTables() {
        createTable("test", "c", "id int key, name varchar(32)");
        createTable("test", "o", "id int key, cid int, odate int, constraint __akiban foreign key(cid) references c(id)");
    }

    @Test
    public void basicCreation() throws InvalidOperationException {
        createTables();
        final String groupName = getUserTable("test", "c").getGroup().getName();
        createGroupIndex(groupName, "name_date", "c.name, o.odate");
        final Group group = ddl().getAIS(session()).getGroup(groupName);
        assertEquals("group index count", 1, group.getIndexes().size());
        final GroupIndex index = group.getIndex("name_date");
        assertNotNull("name_date index exists", index);
        assertEquals("index column count", 2, index.getColumns().size());
        assertEquals("name is first", "name", index.getColumns().get(0).getColumn().getName());
        assertEquals("odate is second", "odate", index.getColumns().get(1).getColumn().getName());

        checkGroupIndexes(getUserTable("test", "c"), index);
        checkGroupIndexes(getUserTable("test", "o"), index);
        checkGroupIndexes(group.getGroupTable(), index);
        // and just to double check...
        assertEquals("c group", group, getUserTable("test", "c").getGroup());
        assertEquals("o group", group, getUserTable("test", "o").getGroup());

    }

    @Test
    public void basicDeletion() throws InvalidOperationException {
        createTables();
        final String groupName = getUserTable("test","c").getGroup().getName();
        createGroupIndex(groupName, "name_date", "c.name, o.odate");
        assertNotNull("name_date exists", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date"));
        ddl().dropGroupIndexes(session(), groupName, Collections.singleton("name_date"));
        assertNull("name_date does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date"));

        checkGroupIndexes(getUserTable("test", "c"));
        checkGroupIndexes(getUserTable("test", "o"));
        checkGroupIndexes(getUserTable("test", "o").getGroup().getGroupTable());
        // and just to double check...
        assertEquals("c group vs o group", getUserTable("test", "o").getGroup(), getUserTable("test", "c").getGroup());
    }

    @Test(expected=InvalidOperationException.class)
    public void tableNotInGroup() throws InvalidOperationException {
        createTables();
        createTable("test", "foo", "id int key, d double");
        final String groupName = getUserTable("test","c").getGroup().getName();
        createGroupIndex(groupName, "name_d", "c.name, foo.d");
    }

    @Test(expected=GroupIndex.GroupIndexCreationException.class)
    public void branchingNotAllowed() throws InvalidOperationException {
        createTables();
        createTable("test", "a", "id int key, cid int, addr int, constraint __akiban foreign key(cid) references c(id)");
        final String groupName = getUserTable("test","c").getGroup().getName();
        createGroupIndex(groupName, "name_addr_date", "c.name, a.addr, o.odate");
    }

    private static void checkGroupIndexes(Table onTable, GroupIndex... indexes) {
        Set<GroupIndex> expected = new HashSet<GroupIndex>(Arrays.asList(indexes));
        Set<GroupIndex> actual = new HashSet<GroupIndex>(onTable.getGroupIndexes());
        assertEquals("group indexes for " + onTable, expected, actual);
    }
}
