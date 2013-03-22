
package com.akiban.server.test.it.dxl;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NewRowBuilder;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.UnsupportedDropException;

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
        final UserTable cTable = ais.getUserTable( ddl().getTableName(session(), cId) );
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
        final UserTable oTable = ais.getUserTable( ddl().getTableName(session(), oId) );
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
        final UserTable iTable = ais.getUserTable( ddl().getTableName(session(), iId) );
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
    public void simple() throws InvalidOperationException {
        createTables(); // TODO placeholder test method until we get the insertToUTablesAndScan to work
    }

    @Test
    public void insertToUTablesAndScan() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c, getRowDef(tids.c)).put(1L).put("Robert").check(session(), dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, getRowDef(tids.o)).put(10L).put(1L).check(session(), dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, getRowDef(tids.i)).put(100L).put(10L).put("Desc 1").check(session(), dml()).row();

        writeRows(cRow, oRow, iRow);
        expectFullRows(tids.c, NewRowBuilder.copyOf(cRow).row());
        expectFullRows(tids.o, NewRowBuilder.copyOf(oRow).row());
        expectFullRows(tids.i, NewRowBuilder.copyOf(iRow).row());
    }

    @Test
    public void insertToUTablesAndScanToLegacy() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c, getRowDef(tids.c)).put(1L).put("Robert").check(session(), dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, getRowDef(tids.o)).put(10L).put(1L).check(session(), dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, getRowDef(tids.i)).put(100L).put(10L).put("Desc 1").check(session(), dml()).row();

        writeRows(cRow, oRow, iRow);
        List<RowData> cRows = scanFull(scanAllRequest(tids.c));
        List<RowData> oRows = scanFull(scanAllRequest(tids.o));
        List<RowData> iRows = scanFull(scanAllRequest(tids.i));

        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("iRows", Arrays.asList(iRow), convertRowDatas(iRows));
    }

    @Test
    public void insertToUTablesBulkAndScanToLegacy() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c, getRowDef(tids.c)).put(1L).put("Robert").check(session(), dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, getRowDef(tids.o)).put(10L).put(1L).check(session(), dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, getRowDef(tids.i)).put(100L).put(10L).put("Desc 1").check(session(), dml()).row();

        dml().writeRows(session(), Arrays.asList(cRow.toRowData(), oRow.toRowData(), iRow.toRowData()));
        List<RowData> cRows = scanFull(scanAllRequest(tids.c));
        List<RowData> oRows = scanFull(scanAllRequest(tids.o));
        List<RowData> iRows = scanFull(scanAllRequest(tids.i));

        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("iRows", Arrays.asList(iRow), convertRowDatas(iRows));
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

        final NewRow cRow = NewRowBuilder.forTable(tids.c, getRowDef(tids.c)).put(1L).put("Robert").check(session(), dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, getRowDef(tids.o)).put(10L).put(1L).check(session(), dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, getRowDef(tids.i)).put(100L).put(10L).put("Desc 1").check(session(), dml()).row();

        writeRows(cRow, oRow, iRow);
        List<RowData> cRows = scanFull(scanAllRequest(tids.c));
        List<RowData> oRows = scanFull(scanAllRequest(tids.o));
        List<RowData> iRows = scanFull(scanAllRequest(tids.i));

        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("iRows", Arrays.asList(iRow), convertRowDatas(iRows));

        ddl().dropTable(session(), tableName(tids.i));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));

        ddl().dropTable(session(), tableName(tids.o));
        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));

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
        final TableName groupName = ddl().getAIS(session()).getUserTable(tableName(tids.i)).getGroup().getName();

        ddl().dropGroup(session(), groupName);

        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNull("expected no table", ais.getUserTable("coi", "c"));
        assertNull("expected no table", ais.getUserTable("coi", "o"));
        assertNull("expected no table", ais.getUserTable("coi", "i"));
        assertNull("expected no group", ais.getGroup(groupName));
    }
}
