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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import org.junit.Test;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NewRowBuilder;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.UnsupportedDropException;

public final class COIBasicIT extends ITBase {
    private static class TableIds {
        public final int c;
        public final int o;
        public final int i;

        private TableIds(int c, int o, int i) {
            this.c = c;
            this.o = o;
            this.i = i;
        }
    }

    private TableIds createTables() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int not null primary key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int not null primary key, c_id int, GROUPING FOREIGN KEY (c_id) REFERENCES c(cid)");
        createIndex("coi", "o", "__akiban_fk_o", "c_id");
        int iId = createTable("coi", "i", "iid int not null primary key, o_id int, idesc varchar(32), GROUPING FOREIGN KEY (o_id) REFERENCES o(oid)");
        createIndex("coi", "i", "__akiban_fk_i", "o_id");
        AkibanInformationSchema ais = ddl().getAIS(session());

        // Lots of checking, the more the merrier
        final Table cTable = ais.getTable( ddl().getTableName(session(), cId) );
        {
            assertEquals("c.columns.size()", 2, cTable.getColumns().size());
            assertEquals("c.indexes.size()", 1, cTable.getIndexes().size());
            PrimaryKey pk = cTable.getPrimaryKey();
            List<Column> expectedPkCols = Arrays.asList( cTable.getColumn("cid") );
            assertEquals("pk cols", expectedPkCols, pk.getColumns());
            assertSame("pk index", cTable.getIndex("PRIMARY"), pk.getIndex());
            assertEquals("pk index cols size", 1, pk.getIndex().getKeyColumns().size());

            assertEquals("parent join", null, cTable.getParentJoin());
            assertEquals("child joins.size", 1, cTable.getChildJoins().size());
        }
        final Table oTable = ais.getTable( ddl().getTableName(session(), oId) );
        {
            assertEquals("c.columns.size()", 2, oTable.getColumns().size());
            assertEquals("c.indexes.size()", 2, oTable.getIndexes().size());
            PrimaryKey pk = oTable.getPrimaryKey();
            List<Column> expectedPkCols = Arrays.asList( oTable.getColumn("oid") );
            assertEquals("pk cols", expectedPkCols, pk.getColumns());
            assertSame("pk index", oTable.getIndex("PRIMARY"), pk.getIndex());
            assertEquals("pk index cols size", 1, pk.getIndex().getKeyColumns().size());

            assertNotNull("parent join is null", oTable.getParentJoin());
            assertSame("parent join", cTable.getChildJoins().get(0), oTable.getParentJoin());
            assertEquals("child joins.size", 1, oTable.getChildJoins().size());
        }
        final Table iTable = ais.getTable( ddl().getTableName(session(), iId) );
        {
            assertEquals("c.columns.size()", 3, iTable.getColumns().size());
            assertEquals("c.indexes.size()", 2, iTable.getIndexes().size());
            PrimaryKey pk = iTable.getPrimaryKey();
            List<Column> expectedPkCols = Arrays.asList( iTable.getColumn("iid") );
            assertEquals("pk cols", expectedPkCols, pk.getColumns());
            assertSame("pk index", iTable.getIndex("PRIMARY"), pk.getIndex());
            assertEquals("pk index cols size", 1, pk.getIndex().getKeyColumns().size());

            assertNotNull("parent join is null", iTable.getParentJoin());
            assertSame("parent join", oTable.getChildJoins().get(0), iTable.getParentJoin());
            assertEquals("child joins.size", 0, iTable.getChildJoins().size());
        }
        {
            Group group = cTable.getGroup();
            assertSame("o's group", group, oTable.getGroup());
            assertSame("i's group", group, iTable.getGroup());
        }

        return new TableIds(cId, oId, iId);
    }

    @Test
    public void simple() {
        createTables();
    }

    @Test
    public void insertToUTablesAndScan() {
        final TableIds tids = createTables();

        final Row cRow = row(tids.c, 1, "Robert");
        final Row oRow = row(tids.o, 10, 1);
        final Row iRow = row(tids.i, 100, 10, "Desc 1");

        writeRows(cRow);
        writeRows(oRow);
        writeRows(iRow);
        expectFullRows(tids.c, cRow);
        expectFullRows(tids.o, oRow);
        expectFullRows(tids.i, iRow);
    }

    @Test(expected=UnsupportedDropException.class)
    public void dropTableRoot() throws InvalidOperationException {
        final TableIds tids = createTables();
        ddl().dropTable(session(), tableName(tids.c));
    }

    @Test(expected=UnsupportedDropException.class)
    public void dropTableMiddle() throws InvalidOperationException {
        final TableIds tids = createTables();
        ddl().dropTable(session(), tableName(tids.o));
    }

    @Test
    public void dropTableLeaves() throws InvalidOperationException {
        final TableIds tids = createTables();

        final Row cRow = row(tids.c, 1, "Robert");
        final Row oRow = row(tids.o, 10, 1);
        final Row iRow = row(tids.i, 100, 10, "Desc 1");

        writeRows(cRow, oRow, iRow);

        expectFullRows(tids.c, cRow);
        expectFullRows(tids.o, oRow);
        expectFullRows(tids.i, iRow);

        ddl().dropTable(session(), tableName(tids.i));
        expectFullRows(tids.c, cRow);
        expectFullRows(tids.o, oRow);

        ddl().dropTable(session(), tableName(tids.o));
        expectFullRows(tids.c, cRow);

        ddl().dropTable(session(), tableName(tids.c));
    }

    @Test
    public void dropAllTablesHelper() throws InvalidOperationException {
        createTables();
        createTable("test", "parent", "id int not null primary key");
        createTable("test", "child", "id int not null primary key, pid int, GROUPING FOREIGN KEY (pid) REFERENCES parent(id)");
        dropAllTables();
    }

    @Test
    public void dropGroup() throws InvalidOperationException {
        final TableIds tids = createTables();
        final TableName groupName = ddl().getAIS(session()).getTable(tableName(tids.i)).getGroup().getName();

        ddl().dropGroup(session(), groupName);

        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNull("expected no table", ais.getTable("coi", "c"));
        assertNull("expected no table", ais.getTable("coi", "o"));
        assertNull("expected no table", ais.getTable("coi", "i"));
        assertNull("expected no group", ais.getGroup(groupName));
    }

    @Test
    public void writeRowHKeyChangePropagation() {
        final TableIds tids = createTables();
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.tableRowType(getTable(tids.c));
        RowType oType = schema.tableRowType(getTable(tids.o));
        RowType iType = schema.tableRowType(getTable(tids.i));
        StoreAdapter adapter = newStoreAdapter(schema);

        Object[] o1Cols = { 10, 1 };
        Object[] cCols = { 2, "c2" };
        Object[] oCols = { 20, 2 };
        Object[] iCols = { 200, 20, "i200" };
        TestRow o1Row = new TestRow(oType, o1Cols);
        TestRow cRow = new TestRow(cType, cCols);
        TestRow oRow = new TestRow(oType, oCols);
        TestRow iRow = new TestRow(iType, iCols);

        // Unrelated o row, to demonstrate i ordering/adoption
        writeRow(tids.o, o1Cols);
        compareRows( new Row[] { o1Row }, adapter.newGroupCursor(cType.table().getGroup()) );

        // i is first due to null cid component
        writeRow(tids.i, iCols);
        compareRows( new Row[] { iRow, o1Row }, adapter.newGroupCursor(cType.table().getGroup()) );

        // i should get adopted by the new o, filling in it's cid component
        writeRow(tids.o, oCols);
        compareRows( new Row[] { o1Row, oRow, iRow, }, adapter.newGroupCursor(cType.table().getGroup()) );

        writeRow(tids.c, cCols);
        compareRows( new Row[] { o1Row, cRow, oRow, iRow }, adapter.newGroupCursor(cType.table().getGroup()) );
    }

    @Test
    public void deleteRowHKeyChangePropagation() {
        final TableIds tids = createTables();
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.tableRowType(getTable(tids.c));
        RowType oType = schema.tableRowType(getTable(tids.o));
        RowType iType = schema.tableRowType(getTable(tids.i));
        StoreAdapter adapter = newStoreAdapter(schema);

        Object[] o1Cols = { 10, 1 };
        Object[] cCols = { 2, "c2" };
        Object[] oCols = { 20, 2 };
        Object[] iCols = { 200, 20, "i200" };
        TestRow o1Row = new TestRow(oType, o1Cols);
        TestRow cRow = new TestRow(cType, cCols);
        TestRow oRow = new TestRow(oType, oCols);
        TestRow iRow = new TestRow(iType, iCols);

        writeRow(tids.o, o1Cols);
        writeRow(tids.c, cCols);
        writeRow(tids.o, oCols);
        writeRow(tids.i, iCols);
        compareRows( new Row[] { o1Row, cRow, oRow, iRow }, adapter.newGroupCursor(cType.table().getGroup()) );

        deleteRow(tids.c, cCols);
        compareRows( new Row[] { o1Row, oRow, iRow }, adapter.newGroupCursor(cType.table().getGroup()) );

        // Delete o => i.cid becomes null
        deleteRow(tids.o, oCols);
        compareRows( new Row[] { iRow, o1Row }, adapter.newGroupCursor(cType.table().getGroup()) );

        deleteRow(tids.i, iCols);
        compareRows( new Row[] { o1Row }, adapter.newGroupCursor(cType.table().getGroup()) );

        deleteRow(tids.o, o1Cols);
        compareRows( new Row[] { }, adapter.newGroupCursor(cType.table().getGroup()) );
    }

    @Test
    public void updateRowHKeyChangePropagation() {
        final TableIds tids = createTables();
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        RowType cType = schema.tableRowType(getTable(tids.c));
        RowType oType = schema.tableRowType(getTable(tids.o));
        RowType iType = schema.tableRowType(getTable(tids.i));
        StoreAdapter adapter = newStoreAdapter(schema);

        Object[] o1Cols = { 10, 1 };
        Object[] cCols = { 2, "c2" };
        Object[] oOrig = { 1, 1 };
        Object[] oUpdate = { 20, 2 };
        Object[] iCols = { 200, 20, "i200" };
        TestRow o1Row = new TestRow(oType, o1Cols);
        TestRow cRow = new TestRow(cType, cCols);
        TestRow oOrigRow = new TestRow(oType, oOrig);
        TestRow oUpdateRow = new TestRow(oType, oUpdate);
        TestRow iRow = new TestRow(iType, iCols);

        writeRow(tids.o, o1Cols);
        writeRow(tids.c, cCols);
        writeRow(tids.o, oOrig);
        writeRow(tids.i, iCols);
        compareRows( new Row[] { iRow, oOrigRow, o1Row, cRow }, adapter.newGroupCursor(cType.table().getGroup()) );

        // updated o moves after o1 and adopts i
    updateRow(oOrigRow, oUpdateRow);
        compareRows( new Row[] { o1Row, cRow, oUpdateRow, iRow }, adapter.newGroupCursor(cType.table().getGroup()) );
    }

    protected void compareRows(Row[] expected, Cursor cursor) {
        txnService().beginTransaction(session());
        try {
            super.compareRows(expected, cursor);
            txnService().commitTransaction(session());
        } finally {
            txnService().rollbackTransactionIfOpen(session());
        }
    }
}
