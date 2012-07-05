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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Table;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.store.IndexKeyVisitor;
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
        cId = createTable("test", "c", "id int not null primary key, name varchar(32)");
        aId = createTable("test", "a", "id int not null primary key, cid int, addr varchar(32), grouping foreign key(cid) references c(id)");
        oId = createTable("test", "o", "id int not null primary key, cid int, odate int, grouping foreign key(cid) references c(id)");
        iId = createTable("test", "i", "id int not null primary key, oid int, sku int, grouping foreign key(oid) references o(id)");
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
        assertEquals("index column count", 2, index.getKeyColumns().size());
        assertEquals("name is first", "name", index.getKeyColumns().get(0).getColumn().getName());
        assertEquals("odate is second", "odate", index.getKeyColumns().get(1).getColumn().getName());

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

    @Test
    public void deletedWhenTableDroppedSpansMultipleTables() throws InvalidOperationException {
        createGroupIndex(groupName, "name_date_sku", "c.name, o.odate, i.sku");
        assertNotNull("name_date_sku exists", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date_sku"));
        ddl().dropTable(session(), tableName(iId));
        assertNull("name_date_sku does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("name_date_sku"));
        checkGroupIndexes(getUserTable("test", "c"));
        checkGroupIndexes(getUserTable("test", "o"));
        checkGroupIndexes(getUserTable("test", "c").getGroup().getGroupTable());
    }

    @Test
    public void deletedWhenTableDroppedSpansOneTableIsChild() throws InvalidOperationException {
        createGroupIndex(groupName, "sku", "i.sku");
        assertNotNull("sku exists", ddl().getAIS(session()).getGroup(groupName).getIndex("sku"));
        ddl().dropTable(session(), tableName(iId));
        assertNull("i doesn't exist", ddl().getAIS(session()).getUserTable("test", "i"));
        assertNull("sku does not exist", ddl().getAIS(session()).getGroup(groupName).getIndex("sku"));
    }

    @Test
    public void deletedWhenTableDroppedSpansOneTableIsRoot() throws InvalidOperationException {
        ddl().dropTable(session(), tableName(iId));
        ddl().dropTable(session(), tableName(oId));
        ddl().dropTable(session(), tableName(aId));
        createGroupIndex(groupName, "name", "c.name");
        assertNotNull("name exists", ddl().getAIS(session()).getGroup(groupName).getIndex("name"));
        ddl().dropTable(session(), tableName(cId));
        assertNull("c doesn't exist", ddl().getAIS(session()).getUserTable("test", "c"));
        assertNull("group does not exist", ddl().getAIS(session()).getGroup(groupName));
    }


    @Test(expected=InvalidOperationException.class)
    public void tableNotInGroup() throws InvalidOperationException {
        createTable("test", "foo", "id int not null primary key, d double");
        createGroupIndex(groupName, "name_d", "c.name, foo.d");
    }

    @Test(expected=BranchingGroupIndexException.class)
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
                            array(Object.class, null, "bar"),
                            array(Object.class, null, "foo"),
                            array(20050930L, "jill"),
                            array(20100702, "bob"),
                            array(20110621, "bob"));
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
                            array(Object.class, null, 20110621L),
                            array(1832L, 20100702L),
                            array(3456L, 20070101L),
                            array(5623L, 20100702L),
                            array(7822L, 20050930L),
                            array(9218L, 20050930L));
    }

    @Test
    public void createACWithExistingData() throws Exception {
        writeRows(createNewRow(cId, 1, "bob"),
                    createNewRow(aId, 3, 1, "123"),
                  createNewRow(cId, 2, "jill"),
                    createNewRow(aId, 1, 2, "875"),
                  createNewRow(cId, 3, "foo"),
                  createNewRow(cId, 4, "bar"),
                    createNewRow(aId, 2, 4, "23"));

        GroupIndex aAddr_cID = createGroupIndex(groupName, "aAddr_cID", "a.addr, c.id");
        expectIndexContents(aAddr_cID,
                            array(Object.class, null, 3L),
                            array(123L, 1L),
                            array(23L, 4L),
                            array(875L, 2L));
    }

    @Test
    public void multipleCreation() throws InvalidOperationException {
        createGroupIndex(groupName, "name_date", "c.name, o.odate");
        createGroupIndex(groupName, "name_sku", "c.name, i.sku");
        final Group group = ddl().getAIS(session()).getGroup(groupName);
        assertEquals("group index count", 2, group.getIndexes().size());
        if (true)
            // TODO: Shouldn't this just happen?
            ddl().getAIS(session()).validate(com.akiban.ais.model.validation.AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();
    }

    private void expectIndexContents(GroupIndex groupIndex, Object[]... keys) throws Exception {
        final Iterator<Object[]> keyIt = Arrays.asList(keys).iterator();
        final List<List<?>> extraKeys = new ArrayList<List<?>>();

        final int declaredColumns = groupIndex.getKeyColumns().size();
        for(Object[] key : keys) {
            assertEquals("Expected key doesn't have declared column count", declaredColumns, key.length);
        }

        final int[] curKey = {0};
        final String indexName = groupIndex.getIndexName().getName();
        persistitStore().traverse(session(), groupIndex, new IndexKeyVisitor() {
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
