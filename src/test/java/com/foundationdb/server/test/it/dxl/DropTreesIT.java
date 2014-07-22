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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.UnsupportedDropException;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.PersistitStoreSchemaManager;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.test.it.ITBase;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class DropTreesIT extends ITBase {
    private boolean treeExists(HasStorage link) throws Exception {
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            boolean exists = store().treeExists(session(), link.getStorageDescription());
            txn.commit();
            return exists;
        }
    }

    private HasStorage treeLink(Object o) {
        if(o == null) throw new IllegalArgumentException("TreeLink holder is null");
        if(o instanceof Table) return ((Table)o).getGroup();
        if(o instanceof Index) return (Index)o;
        throw new IllegalArgumentException("Unknown TreeLink holder: " + o);
    }

    private void expectTree(Object hasTreeLink) throws Exception {
        HasStorage link = treeLink(hasTreeLink);
        assertTrue("tree should exist: " + link.getStorageNameString(), treeExists(link));
    }

    private void expectNoTree(Object hasTreeLink) throws Exception {
        cleanupTrees();
        HasStorage link = treeLink(hasTreeLink);
        assertFalse("tree should not exist: " + link.getStorageNameString(), treeExists(link));
    }

    void cleanupTrees() {
        SchemaManager schemaManager = serviceManager().getSchemaManager();
        if(schemaManager instanceof PersistitStoreSchemaManager) {
            PersistitStoreSchemaManager pssm = (PersistitStoreSchemaManager)schemaManager;
            try {
                pssm.cleanupDelayedTrees(session(), false);
            } catch(PersistitException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Index createSimpleIndex(Table curTable, String columnName) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Table newTable = Table.create(ais, curTable.getName().getSchemaName(), curTable.getName().getTableName(), 0);
        Index newIndex = TableIndex.create(ais, newTable, columnName, 0, false, Index.KEY_CONSTRAINT);
        Column curColumn = curTable.getColumn(columnName);
        Column newColumn = Column.create(newTable,  curColumn.getName(), curColumn.getPosition(), curColumn.getType());
        IndexColumn.create(newIndex, newColumn, 0, true, null);
        return newIndex;
    }

    
    @Test
    public void singleTableNoData() throws Exception {
        int tid = createTable("s", "t", "id int not null primary key");
        Table t = getTable(tid);
        ddl().dropTable(session(), t.getName());
        expectNoTree(t);
    }

    @Test
    public void singleTableNoDataRepeatedly() throws Exception {
        final TableName name = new TableName("s", "t");
        for(int i = 1; i <= 5; ++i) {
            try {
                int tid = createTable(name.getSchemaName(), name.getTableName(), "id int not null primary key");
                Table t = getTable(tid);
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
        Table t = getTable(tid);
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
        Table p = getTable(pid);
        Table c = getTable(cid);
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
        Table p = getTable(pid);
        expectTree(p);
        Table c = getTable(cid);
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
        Table t = getTable(tid);
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
        Table t = getTable(tid);
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
        Table t = getTable(tid);
        ddl().createIndexes(session(), Collections.singleton(createSimpleIndex(t, "other")));
        t = getTable(tid);
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
        Table t = getTable(tid);
        expectTree(t);
        ddl().createIndexes(session(), Collections.singleton(createSimpleIndex(t, "other")));
        t = getTable(tid);
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
        Table t = getTable(tid);
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
        Table t = getTable(tid);
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
        Table p = getTable(pid);
        Table c = getTable(cid);
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
        Table p = getTable(pid);
        expectTree(p);
        writeRows(createNewRow(cid, 10L, 100L, 1L),
                  createNewRow(cid, 20L, 100L, 2L));
        Table c = getTable(cid);
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
        Table t = getTable(tid);
        Index pk = t.getIndexIncludingInternal(Index.PRIMARY_KEY_CONSTRAINT);
        ddl().dropTable(session(), t.getName());
        expectNoTree(pk);
        expectNoTree(t);
    }

    @Test
    public void pkLessRootWithData() throws Exception {
        int tid = createTable("s", "t", "i int");
        createIndex("s", "t", "i", "i");
        writeRows(createNewRow(tid, 10L),
                  createNewRow(tid, 20L));
        Table t = getTable(tid);
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
        Table p = getTable(pid);
        Table c = getTable(cid);
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
        Table p = getTable(pid);
        expectTree(p);
        writeRows(createNewRow(cid, 10L, 1L),
                  createNewRow(cid, 20L, 2L));
        Table c = getTable(cid);
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
        Table p = getTable(pid);
        expectTree(p);
        Index o = p.getIndex("o");
        expectTree(o);
        Table c = getTable(cid);
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
        Table p = getTable(pid);
        Table c = getTable(cid);
        Index index = createLeftGroupIndex(p.getGroup().getName(), "name_val", "p.name", "c.val");
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
        Table p = getTable(pid);
        Table c = getTable(cid);
        Index index = createLeftGroupIndex(p.getGroup().getName(), "name_val", "p.name", "c.val");
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
        Table p = getTable(pid);
        Table c = getTable(cid);
        Index index = createLeftGroupIndex(p.getGroup().getName(), "name_val", "p.name", "c.val");
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
        Table p = getTable(pid);
        Table c = getTable(cid);
        Index index = createLeftGroupIndex(p.getGroup().getName(), "name_val", "p.name", "c.val");
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
        int cid = createTable("s", "c", "cid int not null primary key, pid int, x int, grouping foreign key(pid) references p(id)");
        Table c = getTable(cid);
        Index index = createLeftGroupIndex(c.getGroup().getName(), "name", "c.x", "p.name");
        ddl().dropTable(session(), c.getName());
        expectNoTree(index);
    }
}
