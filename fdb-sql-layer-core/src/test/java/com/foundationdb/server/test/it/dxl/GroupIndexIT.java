/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.BranchingGroupIndexException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.IndexKeyVisitor;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Assert;
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
    private TableName groupName;

    @Before
    public void createTables() {
        cId = createTable("test", "c", "id int not null primary key, name varchar(32)");
        aId = createTable("test", "a", "id int not null primary key, cid int, addr varchar(32), grouping foreign key(cid) references c(id)");
        oId = createTable("test", "o", "id int not null primary key, cid int, odate int, grouping foreign key(cid) references c(id)");
        iId = createTable("test", "i", "id int not null primary key, oid int, sku int, grouping foreign key(oid) references o(id)");
        groupName = getTable(cId).getGroup().getName();
    }

    @After
    public void removeTables() {
        ddl().dropGroup(session(), groupName);
        cId = aId = oId = iId = -1;
        groupName = null;
    }

    @Test
    public void basicCreation() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "name_date", "c.name", "o.odate");
        final Group group = ddl().getAIS(session()).getGroup(groupName);
        assertEquals("group index count", 1, group.getIndexes().size());
        final GroupIndex index = group.getIndex("name_date");
        assertNotNull("name_date index exists", index);
        assertEquals("index column count", 2, index.getKeyColumns().size());
        assertEquals("name is first", "name", index.getKeyColumns().get(0).getColumn().getName());
        assertEquals("odate is second", "odate", index.getKeyColumns().get(1).getColumn().getName());

        checkGroupIndexes(getTable("test", "c"), index);
        checkGroupIndexes(getTable("test", "o"), index);
        // and just to double check...
        assertEquals("c group", group, getTable("test", "c").getGroup());
        assertEquals("o group", group, getTable("test", "o").getGroup());

    }

    @Test
    public void basicDeletion() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "name_date", "c.name", "o.odate");
        assertNotNull("name_date exists", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date"));
        ddl().dropGroupIndexes(session(), groupName, Collections.singleton("name_date"));
        assertNull("name_date does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date"));

        checkGroupIndexes(getTable("test", "c"));
        checkGroupIndexes(getTable("test", "o"));
        // and just to double check...
        assertEquals("c group vs o group", getTable("test", "o").getGroup(), getTable("test", "c").getGroup());
    }

    @Test
    public void deletedWhenTableDroppedSpansMultipleTables() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "name_date_sku", "c.name", "o.odate", "i.sku");
        assertNotNull("name_date_sku exists", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date_sku"));
        ddl().dropTable(session(), tableName(iId));
        assertNull("name_date_sku does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date_sku"));
        checkGroupIndexes(getTable("test", "c"));
        checkGroupIndexes(getTable("test", "o"));
    }

    @Test
    public void deletedWhenTableDroppedSpansOneTableIsChild() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "sku", "i.sku");
        assertNotNull("sku exists", ddl().getAIS(session()).getGroup(groupName).getIndex("sku"));
        ddl().dropTable(session(), tableName(iId));
        assertNull("i doesn't exist", ddl().getAIS(session()).getTable("test", "i"));
        assertNull("sku does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("sku"));
    }

    @Test(expected=InvalidOperationException.class)
    public void tableNotInGroup() throws InvalidOperationException {
        createTable("test", "foo", "id int not null primary key, d double");
        createLeftGroupIndex(groupName, "name_d", "c.name", "foo.d");
    }

    @Test(expected=BranchingGroupIndexException.class)
    public void branchingNotAllowed() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "name_addr_date", "c.name", "a.addr", "o.odate");
    }

    @Test
    public void createCOWithExistingData() throws Exception {
        writeRows(row(cId, 1, "bob"),
                    row(oId, 1, 1, 20100702),
                    row(oId, 2, 1, 20110621),
                  row(cId, 2, "jill"),
                    row(oId, 3, 2, 20050930),
                  row(cId, 3, "foo"),
                  row(cId, 4, "bar"));

        GroupIndex oDate_cName = createLeftGroupIndex(groupName, "oDate_cName", "o.odate", "c.name");
        expectIndexContents(oDate_cName,
                            array(Object.class, null, "bar"),
                            array(Object.class, null, "foo"),
                            array(20050930L, "jill"),
                            array(20100702, "bob"),
                            array(20110621, "bob"));
    }

    @Test
    public void createIOWithExistingData() throws Exception {
        writeRows(row(cId, 1, "bob"),
                    row(oId, 1, 1, 20100702),
                      row(iId, 1, 1, 5623),
                      row(iId, 2, 1, 1832),
                    row(oId, 2, 1, 20110621),
                  row(cId, 2, "jill"),
                    row(oId, 3, 2, 20050930),
                      row(iId, 3, 3, 9218),
                      row(iId, 4, 3, 7822),
                  row(cId, 3, "foo"),
                  row(cId, 4, "bar"),
                    row(oId, 4, 4, 20070101),
                      row(iId, 5, 4, 3456L));

        GroupIndex iSku_oDate = createLeftGroupIndex(groupName, "iSku_oDate", "i.sku", "o.odate");
        expectIndexContents(iSku_oDate,
                            array(Object.class, null, 20110621L),
                            array(1832L, 20100702L),
                            array(3456L, 20070101L),
                            array(5623L, 20100702L),
                            array(7822L, 20050930L),
                            array(9218L, 20050930L));
    }

    @Test
    public void createACWithExistingData() throws Exception {
        writeRows(row(cId, 1, "bob"),
                    row(aId, 3, 1, "123"),
                  row(cId, 2, "jill"),
                    row(aId, 1, 2, "875"),
                  row(cId, 3, "foo"),
                  row(cId, 4, "bar"),
                    row(aId, 2, 4, "23"));

        GroupIndex aAddr_cID = createLeftGroupIndex(groupName, "aAddr_cID", "a.addr", "c.id");
        expectIndexContents(aAddr_cID,
                            array(Object.class, null, 3L),
                            array(123L, 1L),
                            array(23L, 4L),
                            array(875L, 2L));
    }

    @Test
    public void multipleCreation() throws InvalidOperationException {
        createLeftGroupIndex(groupName, "name_date", "c.name", "o.odate");
        createLeftGroupIndex(groupName, "name_sku", "c.name", "i.sku");
        final Group group = ddl().getAIS(session()).getGroup(groupName);
        assertEquals("group index count", 2, group.getIndexes().size());
    }

    private void expectIndexContents(GroupIndex groupIndex, Object[]... keys) throws Exception {
        final Iterator<Object[]> keyIt = Arrays.asList(keys).iterator();
        final List<List<?>> extraKeys = new ArrayList<>();

        final int declaredColumns = groupIndex.getKeyColumns().size();
        for(Object[] key : keys) {
            assertEquals("Expected key doesn't have declared column count", declaredColumns, key.length);
        }

        final int[] curKey = {0};
        final String indexName = groupIndex.getIndexName().getName();
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            store().traverse(session(), groupIndex, new IndexKeyVisitor() {
                @Override
                public boolean groupIndex()
                {
                    return true;
                }

                @Override
                protected void visit(List<?> actual) {
                    if(!keyIt.hasNext()) {
                        extraKeys.add(actual);
                    }
                    else {
                        List<Object> expected = Arrays.asList(keyIt.next());
                        List<?> actualOfDeclared = actual.subList(0, declaredColumns);
                        assertEquals(String.format("Key entry %d of index %s", curKey[0], indexName),
                                     expected.toString(), actualOfDeclared.toString());
                        curKey[0]++;
                    }
                }
            }, -1, 0);
            txn.commit();
        }

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
        Set<GroupIndex> expected = new HashSet<>(Arrays.asList(indexes));
        Set<GroupIndex> actual = new HashSet<>(onTable.getGroupIndexes());
        assertEquals("group indexes for " + onTable, expected, actual);
    }
}
