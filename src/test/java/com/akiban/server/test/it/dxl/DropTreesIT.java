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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.UnsupportedDropException;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.test.it.ITBase;
import com.persistit.exception.PersistitException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public final class DropTreesIT extends ITBase {
    private boolean treeExists(TreeLink link) throws Exception {
        return serviceManager().getTreeService().treeExists(link.getSchemaName(), link.getTreeName());
    }

    private TreeLink treeLink(Object o) {
        if(o == null) throw new IllegalArgumentException("TreeLink holder is null");
        if(o instanceof Table) return ((Table)o).getGroup();
        if(o instanceof Index) return ((Index)o).indexDef();
        throw new IllegalArgumentException("Unknown TreeLink holder: " + o);
    }

    private void expectTree(Object hasTreeLink) throws Exception {
        TreeLink link = treeLink(hasTreeLink);
        assertTrue("tree should exist: " + link.getTreeName(), treeExists(link));
    }

    private void expectNoTree(Object hasTreeLink) throws Exception {
        cleanupTrees();
        TreeLink link = treeLink(hasTreeLink);
        assertFalse("tree should not exist: " + link.getTreeName(), treeExists(link));
    }

    void cleanupTrees() {
        SchemaManager schemaManager = serviceManager().getSchemaManager();
        if(schemaManager instanceof PersistitStoreSchemaManager) {
            PersistitStoreSchemaManager pssm = (PersistitStoreSchemaManager)schemaManager;
            try {
                pssm.cleanupDelayedTrees(session());
            } catch(PersistitException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Index createSimpleIndex(Table curTable, String columnName) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Table newTable = UserTable.create(ais, curTable.getName().getSchemaName(), curTable.getName().getTableName(), 0);
        Index newIndex = TableIndex.create(ais, newTable, columnName, 0, false, Index.KEY_CONSTRAINT);
        Column curColumn = curTable.getColumn(columnName);
        Column newColumn = Column.create(newTable,  curColumn.getName(), curColumn.getPosition(), curColumn.getType());
        newColumn.setTypeParameter1(curColumn.getTypeParameter1());
        newColumn.setTypeParameter2(curColumn.getTypeParameter2());
        IndexColumn.create(newIndex, newColumn, 0, true, null);
        return newIndex;
    }

    
    @Test
    public void singleTableNoData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key");
        Table t = getUserTable(tid);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
    }

    @Test
    public void singleTableNoDataRepeatedly() throws Exception {
        final TableName name = new TableName("s", "t");
        for(int i = 1; i <= 5; ++i) {
            try {
                int tid = createTable(name.getSchemaName(), name.getTableName(), "id int not null primary key");
                Table t = getUserTable(tid);
                ddl().dropTable(session(), name);
                expectNoTree(t);
            } catch(Exception e) {
                throw new Exception("Failed on iteration: " + i, e);
            }
        }
    }

    @Test
    public void singleTableWithData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, name varchar(32)");
        Table t = getUserTable(tid);
        writeRows(createNewRow(tid, 1L, "joe"),
                  createNewRow(tid, 2L, "bob"),
                  createNewRow(tid, 3L, "jim"));
        expectTree(t);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
    }

    @Test
    public void groupedTablesNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        ddl().dropTable(session(), c.getName());
        ddl().dropTable(session(), p.getName());
        expectNoTree(p);
        expectNoTree(c);
    }

    @Test
    public void groupedTablesWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
        writeRows(createNewRow(pid, 1L),
                  createNewRow(pid, 2L),
                  createNewRow(pid, 3L));
        writeRows(createNewRow(cid, 10L, 1L),
                  createNewRow(cid, 20L, 1L),
                  createNewRow(cid, 30L, 2L));
        Table p = getUserTable(pid);
        expectTree(p);
        Table c = getUserTable(cid);
        expectTree(c);
        ddl().dropTable(session(), c.getName());
        ddl().dropTable(session(), p.getName());
        expectNoTree(p);
        expectNoTree(c);
    }

    @Test
    public void secondaryIndexNoData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, c char(10)");
        createIndex("s", "t", "c", "c");
        Table t = getUserTable(tid);
        Index c = t.getIndex("c");
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
        expectNoTree(c);
    }

    @Test
    public void secondaryIndexWithData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, c char(10)");
        createIndex("s", "t", "c", "c");
        writeRows(createNewRow(tid, 1L, "abcd"),
                  createNewRow(tid, 2L, "efgh"),
                  createNewRow(tid, 3L, "ijkl"));
        Table t = getUserTable(tid);
        expectTree(t);
        Index c = t.getIndex("c");
        expectTree(c);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
        expectNoTree(c);
    }

    @Test
    public void addSecondaryIndexNoData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, other int");
        Table t = getUserTable(tid);
        ddl().createIndexes(session(), Collections.singleton(createSimpleIndex(t, "other")));
        t = getUserTable(tid);
        Index other = t.getIndex("other");
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
        expectNoTree(other);
    }

    @Test
    public void addSecondaryIndexWithData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, other int");
        writeRows(createNewRow(tid, 1L, 10L),
                  createNewRow(tid, 2L, 20L),
                  createNewRow(tid, 3L, 30L));
        Table t = getUserTable(tid);
        expectTree(t);
        ddl().createIndexes(session(), Collections.singleton(createSimpleIndex(t, "other")));
        t = getUserTable(tid);
        expectTree(t);
        Index other = t.getIndex("other");
        expectTree(other);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
        expectNoTree(other);
    }

    @Test
    public void dropSecondaryIndexNoData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, c char(10)");
        createIndex("s", "t", "c", "c");
        createIndex("s", "t", "c2", "c");
        Table t = getUserTable(tid);
        Index c = t.getIndex("c");
        ddl().dropTableIndexes(session(), t.getName(), Collections.singleton("c"));
        expectNoTree(c);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
    }
    
    @Test
    public void dropSecondaryIndexWithData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key, c char(10)");
        createIndex("s", "t", "c", "c");
        createIndex("s", "t", "c2", "c");
        writeRows(createNewRow(tid, 1L, "mnop"),
                  createNewRow(tid, 2L, "qrst"),
                  createNewRow(tid, 3L, "uvwx"));
        Table t = getUserTable(tid);
        expectTree(t);
        Index c = t.getIndex("c");
        expectTree(c);
        ddl().dropTableIndexes(session(), t.getName(), Collections.singleton("c"));
        expectNoTree(c);
        expectTree(t);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
    }

    @Test
    public void childSecondaryIndexNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "id int not null primary key, i int, pid int, grouping foreign key(pid) references p(id)");
        createIndex("s", "c", "i", "i");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        Index i = c.getIndex("i");
        ddl().dropTable(session(), c.getName());
        expectNoTree(i);
        ddl().dropTable(session(), p.getName());
        expectNoTree(p);
    }

    @Test
    public void childSecondaryIndexWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "id int not null primary key, i int, pid int, grouping foreign key(pid) references p(id)");
        createIndex("s", "c", "i", "i");
        writeRows(createNewRow(pid, 1L),
                  createNewRow(pid, 2L));
        Table p = getUserTable(pid);
        expectTree(p);
        writeRows(createNewRow(cid, 10L, 100L, 1L),
                  createNewRow(cid, 20L, 100L, 2L));
        Table c = getUserTable(cid);
        expectTree(c);
        Index i = c.getIndex("i");
        expectTree(i);
        ddl().dropTable(session(), c.getName());
        expectNoTree(i);
        ddl().dropTable(session(), p.getName());
        expectNoTree(p);
    }

    @Test
    public void pkLessRootNoData() throws Exception {
        int tid = createTable("s", "t", "i int");
        createIndex("s", "t", "i", "i");
        UserTable t = getUserTable(tid);
        Index pk = t.getIndexIncludingInternal(Index.PRIMARY_KEY_CONSTRAINT);
        ddl().dropTable(session(), t.getName());
        expectNoTree(pk);
        expectNoTree(t);
    }

    @Test
    public void pkLessRootWithData() throws Exception {
        int tid = createTable("s", "t", "i int");
        createIndex("s", "t", "i", "i");
        writeRows(createNewRow(tid, 10L, 0L),
                  createNewRow(tid, 20L, 0L));
        UserTable t = getUserTable(tid);
        expectTree(t);
        Index pk = t.getIndexIncludingInternal(Index.PRIMARY_KEY_CONSTRAINT);
        expectTree(pk);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
        expectNoTree(pk);
    }

    @Test
    public void pkLessChildNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "i int, pid int, grouping foreign key(pid) references p(id)");
        createIndex("s", "c", "i", "i");
        Table p = getUserTable(pid);
        UserTable c = getUserTable(cid);
        Index pk = c.getIndexIncludingInternal(Index.PRIMARY_KEY_CONSTRAINT);
        ddl().dropTable(session(), c.getName());
        expectNoTree(pk);
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void pkLessChildTableWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key");
        int cid = createTable("s", "c", "i int, pid int, grouping foreign key(pid) references p(id)");
        createIndex("s", "c", "i", "i");
        writeRows(createNewRow(pid, 1L),
                  createNewRow(pid, 2L));
        Table p = getUserTable(pid);
        expectTree(p);
        writeRows(createNewRow(cid, 10L, 1L, 0L),
                  createNewRow(cid, 20L, 2L, 0L));
        UserTable c = getUserTable(cid);
        expectTree(c);
        Index pk = c.getIndexIncludingInternal(Index.PRIMARY_KEY_CONSTRAINT);
        expectTree(pk);
        ddl().dropTable(session(), c.getName());
        expectNoTree(pk);
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void groupedTablesWithDataTryDropParent() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, o int");
        createIndex("s", "p", "o", "o");
        int cid = createTable("s", "c", "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
        writeRows(createNewRow(pid, 1L, 100L),
                  createNewRow(pid, 2L, 200L),
                  createNewRow(pid, 3L, 300L));
        writeRows(createNewRow(cid, 10L, 1L),
                  createNewRow(cid, 20L, 1L),
                  createNewRow(cid, 30L, 2L));
        Table p = getUserTable(pid);
        expectTree(p);
        Index o = p.getIndex("o");
        expectTree(o);
        Table c = getUserTable(cid);
        expectTree(c);
        try {
            ddl().dropTable(session(), p.getName());
            fail("Expected UnsupportedDropException!");
        } catch(UnsupportedDropException e) {
            expectTree(p);
            expectTree(o);
            expectTree(c);
        }
        ddl().dropTable(session(), c.getName());
        ddl().dropTable(session(), p.getName());
        expectNoTree(p);
        expectNoTree(o);
        expectNoTree(c);
    }

    @Test
    public void dropTableInGroupIndexWithNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        int cid = createTable("s", "c", "id int not null primary key, pid int, val int, grouping foreign key(pid) references p(id)");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        Index index = createGroupIndex(p.getGroup().getName(), "name_val", "p.name,c.val");
        ddl().dropTable(session(), c.getName());
        expectNoTree(index);
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void dropTableInGroupIndexWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        int cid = createTable("s", "c", "id int not null primary key, pid int, val int, grouping foreign key(pid) references p(id)");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        Index index = createGroupIndex(p.getGroup().getName(), "name_val", "p.name,c.val");
        writeRows(createNewRow(pid, 1, "bob"),
                    createNewRow(cid, 1, 1, 100),
                    createNewRow(cid, 2, 1, 101),
                  createNewRow(pid, 2, "joe"),
                    createNewRow(cid, 3, 2, 102),
                  createNewRow(pid, 3, "foo"));
        ddl().dropTable(session(), c.getName());
        expectNoTree(index);
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void dropGroupIndexWithNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        int cid = createTable("s", "c", "id int not null primary key, pid int, val int, grouping foreign key(pid) references p(id)");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        Index index = createGroupIndex(p.getGroup().getName(), "name_val", "p.name,c.val");
        ddl().dropGroupIndexes(session(), p.getGroup().getName(), Collections.singleton("name_val"));
        ddl().dropTable(session(), c.getName());
        expectNoTree(index);
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void dropGroupIndexWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        int cid = createTable("s", "c", "id int not null primary key, pid int, val int, grouping foreign key(pid) references p(id)");
        Table p = getUserTable(pid);
        Table c = getUserTable(cid);
        Index index = createGroupIndex(p.getGroup().getName(), "name_val", "p.name,c.val");
        writeRows(createNewRow(pid, 1, "bob"),
                    createNewRow(cid, 1, 1, 100),
                    createNewRow(cid, 2, 1, 101),
                  createNewRow(pid, 2, "joe"),
                    createNewRow(cid, 3, 2, 102),
                  createNewRow(pid, 3, "foo"));
        ddl().dropGroupIndexes(session(), p.getGroup().getName(), Collections.singleton("name_val"));
        expectNoTree(index);
        ddl().dropTable(session(), c.getName());
        ddl().dropTable(session(), p.getName());
        expectNoTree(c);
        expectNoTree(p);
    }

    @Test
    public void dropSingleTableWithGroupIndexWithNoData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        Table p = getUserTable(pid);
        Index index = createGroupIndex(p.getGroup().getName(), "name", "p.name");
        ddl().dropTable(session(), p.getName());
        expectNoTree(index);
        expectNoTree(p);
    }

    @Ignore("Can't write rows to group index that is on single/root table?")
    @Test
    public void dropSingleTableWithGroupIndexWithData() throws Exception {
        int pid = createTable("s", "p", "id int not null primary key, name varchar(32)");
        Table p = getUserTable(pid);
        Index index = createGroupIndex(p.getGroup().getName(), "name", "p.name");
        writeRows(createNewRow(pid, 1, "foo"),
                  createNewRow(pid, 2, "bar"));
        expectTree(p);
        expectTree(index);
        ddl().dropTable(session(), p.getName());
        expectNoTree(index);
        expectNoTree(p);
    }
}
