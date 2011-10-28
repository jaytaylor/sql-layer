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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NewRowBuilder;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.UnsupportedDropException;

public final class COIBasicIT extends ITBase {
    private static class TableIds {
        public final int c;
        public final int o;
        public final int i;
        public final int coi;

        private TableIds(int c, int o, int i, int coi) {
            this.c = c;
            this.o = o;
            this.i = i;
            this.coi = coi;
        }
    }

    private TableIds createTables() throws InvalidOperationException {
        int cId = createTable("coi", "c", "cid int key, name varchar(32)");
        int oId = createTable("coi", "o", "oid int key, c_id int, CONSTRAINT __akiban_fk_o FOREIGN KEY index1 (c_id) REFERENCES c(cid)");
        int iId = createTable("coi", "i", "iid int key, o_id int, idesc varchar(32), CONSTRAINT __akiban_fk_i FOREIGN KEY index2 (o_id) REFERENCES o(oid)");
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
            assertEquals("pk index cols size", 1, pk.getIndex().getColumns().size());

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
            assertEquals("pk index cols size", 1, pk.getIndex().getColumns().size());

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
            assertEquals("pk index cols size", 1, pk.getIndex().getColumns().size());

            assertNotNull("parent join is null", iTable.getParentJoin());
            assertSame("parent join", oTable.getChildJoins().get(0), iTable.getParentJoin());
            assertEquals("child joins.size", 0, iTable.getChildJoins().size());
        }
        final GroupTable gTable;
        {
            Group group = cTable.getGroup();
            assertSame("o's group", group, oTable.getGroup());
            assertSame("i's group", group, iTable.getGroup());
            gTable = group.getGroupTable();

            assertEquals("group table's columns size", 7, gTable.getColumns().size());
            assertEquals("group table's indexes size", 5, gTable.getIndexes().size());
        }

        return new TableIds(cId, oId, iId, gTable.getTableId());
    }

    @Test
    public void simple() throws InvalidOperationException {
        createTables(); // TODO placeholder test method until we get the insertToUTablesAndScan to work
    }

    @Test
    public void insertToUTablesAndScan() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c, store()).put(1L).put("Robert").check(dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, store()).put(10L).put(1L).check(dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, store()).put(100L).put(10L).put("Desc 1").check(dml()).row();

        writeRows(cRow, oRow, iRow);
        expectFullRows(tids.c, NewRowBuilder.copyOf(cRow, store()).row());
        expectFullRows(tids.o, NewRowBuilder.copyOf(oRow, store()).row());
        expectFullRows(tids.i, NewRowBuilder.copyOf(iRow, store()).row());

//        expectFullRows(tids.coi, cRow, oRow, iRow); // TODO - commented out per 751883
    }

    @Test
    public void insertToUTablesAndScanToLegacy() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c, store()).put(1L).put("Robert").check(dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, store()).put(10L).put(1L).check(dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, store()).put(100L).put(10L).put("Desc 1").check(dml()).row();

        writeRows(cRow, oRow, iRow);
        List<RowData> cRows = scanFull(scanAllRequest(tids.c));
        List<RowData> oRows = scanFull(scanAllRequest(tids.o));
        List<RowData> iRows = scanFull(scanAllRequest(tids.i));

        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("iRows", Arrays.asList(iRow), convertRowDatas(iRows));

//        expectFullRows(tids.coi, cRow, oRow, iRow); // TODO - commented out per 751883
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

        final NewRow cRow = NewRowBuilder.forTable(tids.c, store()).put(1L).put("Robert").check(dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o, store()).put(10L).put(1L).check(dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i, store()).put(100L).put(10L).put("Desc 1").check(dml()).row();

        writeRows(cRow, oRow, iRow);
        List<RowData> cRows = scanFull(scanAllRequest(tids.c));
        List<RowData> oRows = scanFull(scanAllRequest(tids.o));
        List<RowData> iRows = scanFull(scanAllRequest(tids.i));

        assertEquals("cRows", Arrays.asList(cRow), convertRowDatas(cRows));
        assertEquals("oRows", Arrays.asList(oRow), convertRowDatas(oRows));
        assertEquals("iRows", Arrays.asList(iRow), convertRowDatas(iRows));

//        expectFullRows(tids.coi, cRow, oRow, iRow); // TODO - commented out per 751883

        ddl().dropTable(session(), tableName(tids.i));
//        expectFullRows(tids.coi, cRow, oRow); // TODO - commented out per 751883

        ddl().dropTable(session(), tableName(tids.o));
//        expectFullRows(tids.coi, cRow); // TODO - commented out per 751883

        ddl().dropTable(session(), tableName(tids.c));

        try {
            expectFullRows(tids.coi);
            assertTrue("group table exists", false);
        }
        catch(NoSuchTableIdException e) {
            // Expected, deleting root table should remove group table
        }
    }

    @Test
    public void dropAllTablesHelper() throws InvalidOperationException {
        createTables();
        createTable("test", "parent", "id int key");
        createTable("test", "child", "id int key, pid int, CONSTRAINT __akiban_fk_0 FOREIGN KEY __akiban_fk_0(pid) REFERENCES parent(id)");
        dropAllTables();
    }

    @Test
    public void dropGroup() throws InvalidOperationException {
        final TableIds tids = createTables();
        final String groupName = ddl().getAIS(session()).getUserTable(tableName(tids.i)).getGroup().getName();

        ddl().dropGroup(session(), groupName);

        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNull("expected no table", ais.getUserTable("coi", "c"));
        assertNull("expected no table", ais.getUserTable("coi", "o"));
        assertNull("expected no table", ais.getUserTable("coi", "i"));
        assertNull("expected no group", ais.getGroup(groupName));
    }
}
