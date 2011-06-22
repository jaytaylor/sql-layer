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
import com.akiban.ais.model.Table;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.store.IndexRecordVisitor;
import com.akiban.server.test.it.ITBase;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GroupIndexIT extends ITBase {
    private int cId;
    private int aId;
    private int oId;
    private int iId;
    private String groupName;

    @Before
    public void createTables() {
        cId = createTable("test", "c", "id int key, name varchar(32)");
        aId = createTable("test", "a", "id int key, cid int, addr varchar(32), constraint __akiban foreign key(cid) references c(id)");
        oId = createTable("test", "o", "id int key, cid int, odate int, constraint __akiban foreign key(cid) references c(id)");
        iId = createTable("test", "i", "id int key, oid int, sku int, constraint __akiban foreign key(oid) references o(id)");
        groupName = getUserTable(cId).getGroup().getName();
    }

    @After
    public void removeTables() {
        ddl().dropGroup(session(), groupName);
        cId = aId = oId = iId = -1;
        groupName = "";
    }

    @Test
    public void basicCreation() throws InvalidOperationException {
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
        createTable("test", "foo", "id int key, d double");
        createGroupIndex(groupName, "name_d", "c.name, foo.d");
    }

    @Test(expected=GroupIndex.GroupIndexCreationException.class)
    public void branchingNotAllowed() throws InvalidOperationException {
        createGroupIndex(groupName, "name_addr_date", "c.name, a.addr, o.odate");
    }

    @Test
    public void createCOWithExistingData() throws Exception {
        writeRows(createNewRow(cId, 1, "bob"),
                    createNewRow(oId, 1, 1, 20100702),
                    createNewRow(oId, 2, 1, 20110621),
                  createNewRow(cId, 2, "jill"),
                    createNewRow(oId, 3, 2, 20050930),
                  createNewRow(cId, 3, "foo"),
                  createNewRow(cId, 4, "bar"));

        GroupIndex oDate_cName = createGroupIndex(groupName, "oDate_cName", "o.odate, c.name");
        expectIndexContents(oDate_cName,
                            array(Object.class, 20050930L, "jill"),
                            array(Object.class, 20100702, "bob"),
                            array(Object.class, 20110621, "bob"));
    }

    @Test
    public void createIOWithExistingData() throws Exception {
        writeRows(createNewRow(cId, 1, "bob"),
                    createNewRow(oId, 1, 1, 20100702),
                      createNewRow(iId, 1, 1, 5623),
                      createNewRow(iId, 2, 1, 1832),
                    createNewRow(oId, 2, 1, 20110621),
                  createNewRow(cId, 2, "jill"),
                    createNewRow(oId, 3, 2, 20050930),
                      createNewRow(iId, 3, 3, 9218),
                      createNewRow(iId, 4, 3, 7822),
                  createNewRow(cId, 3, "foo"),
                  createNewRow(cId, 4, "bar"),
                    createNewRow(oId, 4, 4, 20070101),
                      createNewRow(iId, 5, 4, 3456L));

        GroupIndex iSku_oDate = createGroupIndex(groupName, "iSku_oDate", "i.sku, o.odate");
        expectIndexContents(iSku_oDate,
                            array(Object.class, 1832L, 20100702L),
                            array(Object.class, 3456L, 20070101L),
                            array(Object.class, 5623L, 20100702L),
                            array(Object.class, 7822L, 20050930L),
                            array(Object.class, 9218L, 20050930L));
    }

    @Test
    public void createACWithExistingData() throws Exception {
        writeRows(createNewRow(cId, 1, "bob"),
                    createNewRow(aId, 3, 1, 123),
                  createNewRow(cId, 2, "jill"),
                    createNewRow(aId, 1, 2, 875),
                  createNewRow(cId, 3, "foo"),
                  createNewRow(cId, 4, "bar"),
                    createNewRow(aId, 2, 4, 23));

        GroupIndex aAddr_cID = createGroupIndex(groupName, "aAddr_cID", "a.addr, c.id");
        expectIndexContents(aAddr_cID,
                            array(Object.class, 123L, 1L),
                            array(Object.class, 23L, 4L),
                            array(Object.class, 875L, 2L));
    }


    private void expectIndexContents(GroupIndex groupIndex, Object[]... keys) throws Exception {
        final Iterator<Object[]> keyIt = Arrays.asList(keys).iterator();
        final List<List<Object>> extraKeys = new ArrayList<List<Object>>();

        final int declaredColumns = groupIndex.getColumns().size();
        for(Object[] key : keys) {
            assertEquals("Expected key doesn't have declared column count", declaredColumns, key.length);
        }

        final int[] curKey = {0};
        final String indexName = groupIndex.getIndexName().getName();
        persistitStore().traverse(session(), groupIndex, new IndexRecordVisitor() {
            @Override
            public void visit(List<Object> actual) {
                if(!keyIt.hasNext()) {
                    extraKeys.add(actual);
                }
                else {
                    List<Object> expected = Arrays.asList(keyIt.next());
                    List<Object> actualOfDeclared = actual.subList(0, declaredColumns);
                    assertEquals(String.format("Key entry %d of index %s", curKey[0], indexName),
                                 expected.toString(), actualOfDeclared.toString());
                    curKey[0]++;
                }
            }
        });

        if(!extraKeys.isEmpty()) {
            Assert.fail(String.format("Extra keys tree for index %s: %s", indexName, extraKeys));
        }
        else if(keyIt.hasNext()) {
            String expectedMoreStr = "";
            for(int i = curKey[0]; i < keys.length; ++i) {
                expectedMoreStr += Arrays.toString(keys[i]) + ",";
            }
            Assert.fail(String.format("Expected more keys in index %s: %s", indexName, expectedMoreStr));
        }
    }

    private static void checkGroupIndexes(Table onTable, GroupIndex... indexes) {
        Set<GroupIndex> expected = new HashSet<GroupIndex>(Arrays.asList(indexes));
        Set<GroupIndex> actual = new HashSet<GroupIndex>(onTable.getGroupIndexes());
        assertEquals("group indexes for " + onTable, expected, actual);
    }
}
