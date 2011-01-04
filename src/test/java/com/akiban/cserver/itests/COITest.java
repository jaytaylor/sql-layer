package com.akiban.cserver.itests;

import com.akiban.ais.model.*;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NewRowBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.*;

public final class COITest extends ApiTestBase {
    private static class TableIds {
        public final TableId c;
        public final TableId o;
        public final TableId i;
        public final TableId coi;

        private TableIds(TableId c, TableId o, TableId i, TableId coi) {
            this.c = c;
            this.o = o;
            this.i = i;
            this.coi = coi;
        }
    }

    private TableIds createTables() throws InvalidOperationException {
        TableId cId = createTable("coi", "c", "cid int key, name varchar(32)");
        TableId oId = createTable("coi", "o", "oid int key, c_id int, CONSTRAINT __akiban_fk_o FOREIGN KEY index1 (c_id) REFERENCES c(cid)");
        TableId iId = createTable("coi", "i", "iid int key, o_id int, idesc varchar(32), CONSTRAINT __akiban_fk_i FOREIGN KEY index2 (o_id) REFERENCES o(oid)");
        AkibaInformationSchema ais = ddl().getAIS();

        // Lots of checking, the more the merrier
        final UserTable cTable = ais.getUserTable( ddl().getTableName(cId) );
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
        final UserTable oTable = ais.getUserTable( ddl().getTableName(oId) );
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
        final UserTable iTable = ais.getUserTable( ddl().getTableName(iId) );
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

        return new TableIds(cId, oId, iId, TableId.of(gTable.getTableId()));
    }

    @Test
    public void simple() throws InvalidOperationException {
        createTables(); // TODO placeholder test method until we get the insertToUTablesAndScan to work
    }

    @Ignore @Test
    public void insertToUTablesAndScan() throws InvalidOperationException {
        final TableIds tids = createTables();

        final NewRow cRow = NewRowBuilder.forTable(tids.c).put(1L).put("Robert").check(dml()).row();
        final NewRow oRow = NewRowBuilder.forTable(tids.o).put(10L).put(1L).check(dml()).row();
        final NewRow iRow = NewRowBuilder.forTable(tids.i).put(100L).put(10L).put("Desc 1").check(dml()).row();

        writeRows(cRow, oRow, iRow);
        expectFullRows(tids.c, NewRowBuilder.copyOf(cRow).row());
        expectFullRows(tids.o, NewRowBuilder.copyOf(oRow).row());
        expectFullRows(tids.i, NewRowBuilder.copyOf(iRow).row());
        
        expectFullRows(tids.coi, cRow, oRow, iRow);
    }
}
