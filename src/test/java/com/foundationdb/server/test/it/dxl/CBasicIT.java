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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.FixedCountLimit;
import com.foundationdb.server.api.dml.scan.*;
import com.foundationdb.server.error.CursorIsFinishedException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchRowException;
import com.foundationdb.server.error.OldAISException;
import com.foundationdb.server.error.RowDefNotFoundException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public final class CBasicIT extends ITBase {

    @Test(expected=RowDefNotFoundException.class)
    public void dropTable() throws InvalidOperationException {
        final int tableId1;
        try {
            tableId1 = createTable("testSchema", "customer", "id int not null primary key");
            ddl().dropTable(session(), tableName("testSchema", "customer"));

            AkibanInformationSchema ais = ddl().getAIS(session());
            assertNull("expected no table", ais.getTable("testSchema", "customer"));
            ddl().dropTable(session(), tableName("testSchema", "customer")); // should be no-op; testing it doesn't fail
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        dml().openCursor(session(), ddl().getGenerationAsInt(session()), new ScanAllRequest(tableId1, ColumnSet.ofPositions(0)));
    }

    @Test(expected=RowDefNotFoundException.class)
    public void dropGroup() throws InvalidOperationException {
        final int tid;
        try {
            tid = createTable("test", "t", "id int not null primary key");
            final TableName groupName = ddl().getAIS(session()).getTable("test", "t").getGroup().getName();
            ddl().dropGroup(session(), groupName);

            AkibanInformationSchema ais = ddl().getAIS(session());
            assertNull("expected no table", ais.getTable("test", "t"));
            assertNull("expected no group", ais.getGroup(groupName));

            ddl().dropGroup(session(), groupName);
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        dml().openCursor(session(), ddl().getGenerationAsInt(session()), new ScanAllRequest(tid, ColumnSet.ofPositions(0)));
    }

    /*
     * Found from an actual case in the MTR test suite. Caused by recycled RowDefIDs and undeleted table statuses.
     * Really testing that table statuses get deleted, but about as direct as we can get from this level.
     */
    @Test
    public void dropThenCreateRowDefIDRecycled() throws InvalidOperationException {
        NewAISBuilder builder = AISBBasedBuilder.create("test", ddl().getTypesTranslator());
        builder.table("t1").autoIncInt("id", 1).pk("id").colString("name", 255);
        ddl().createTable(session(), builder.ais().getTable("test", "t1"));
        final int tidV1 = tableId("test", "t1");

        writeRow(tidV1, 1, "hello world");
        expectRowCount(tidV1, 1);
        ddl().dropTable(session(), tableName(tidV1));

        // Easiest exception trigger was to toggle auto_inc column, failed when trying to update it
        final int tidV2 = createTable("test", "t2", "id int not null primary key, tag char(1), value decimal(10,2)");
        writeRow(tidV2, "1", "a", "49.95");
        expectRowCount(tidV2, 1);
        ddl().dropTable(session(), tableName(tidV2));
    }

    @Test
    public void scanEmptyTable() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        ScanRequest request = new ScanAllRequest(tableId, ColumnSet.ofPositions(0, 1), 0, null, new FixedCountLimit(1));
        ListRowOutput output = new ListRowOutput();

        assertEquals("cursors", cursorSet(), dml().getCursors(session()));
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session()));
        assertEquals("state", CursorState.FRESH, dml().getCursorState(session(), cursorId));

        dml().scanSome(session(), cursorId, output);
        assertEquals("state", CursorState.FINISHED, dml().getCursorState(session(), cursorId));

        CursorIsFinishedException caught = null;
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (CursorIsFinishedException e) {
            caught = e;
        }
        assertNotNull("expected an exception", caught);

        assertEquals("cursors", cursorSet(cursorId), dml().getCursors(session()));
        dml().closeCursor(session(), cursorId);
        assertEquals("cursors", cursorSet(), dml().getCursors(session()));

        assertEquals("rows scanned", Collections.<NewRow>emptyList(), output.getRows());
    }

    @Test
    public void updateNoChangeToHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 0, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 0, "goodbye cruel world") );
    }

    @Test
    public void updateOldOnlyById() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 1, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, "goodbye cruel world") );
    }

    @Test(expected=NoRowsUpdatedException.class)
    public void updateOldNotById() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

            expectRowCount(tableId, 0);
            writeRow(tableId, 0, "hello world");
            expectRowCount(tableId, 1);

            expectRows(tableId, row(tableId, 0, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        Row badRow = row(tableId, 1, "goodbye cruel world");
        try {
            Row old = row(tableId, null, "hello world");
            updateRow(old, badRow);
        } catch (NoSuchRowException e) {
            expectRows(tableId, row(tableId, 0, "hello world"));
            throw new NoRowsUpdatedException();
        }
    }

    /**
     * We currently can't differentiate between null and unspecified, so not specifying a field is the same as
     * setting it null. Thus, by providing a truncated definition for both old and new rows, we're essentially
     * nulling some of the row as well as shortening it.
     * @throws InvalidOperationException if there's a failure
     */
    @Test
    public void updateRowPartially() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, null), row(tableId, 1, null));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, null) );
    }

    @Test
    public void updateChangesHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 1, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, "goodbye cruel world") );
    }

    @Test
    public void deleteRows() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "doomed row");
        expectRowCount(tableId, 1);
        writeRow(tableId, 1, "also doomed");
        expectRowCount(tableId, 2);

        expectRows(tableId,
                row(tableId, 0, "doomed row"),
                row(tableId, 1, "also doomed"));

        deleteRow(tableId, 0, "doomed row");
        expectRowCount(tableId, 1);
        expectRows(tableId,
                row(tableId, 1, "also doomed"));

        deleteRow(tableId, 1, "also doomed");
        expectRowCount(tableId, 0);
        expectRows(tableId);
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteRowNotById() throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int not null primary key, name varchar(32)");

            expectRowCount(tableId, 0);
            writeRow(tableId, 0, "the customer's name");
            expectRowCount(tableId, 1);

            expectRows(tableId, row(tableId, 0, "the customer's name"));
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            Row deleteAttempt = row(tableId, null, "the customer's name");
            deleteRow(deleteAttempt);
        } catch (NoSuchRowException e) {
            expectRows(tableId, row(tableId, 0, "the customer's name"));
            throw e;
        }
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteMissingRow()  throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int not null primary key, name varchar(32)");
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            deleteRow(tableId, 0, "this row doesn't exist");
        } catch (NoSuchRowException e) {
            expectRows(tableId);
            throw e;
        }
    }

    @Test
    public void schemaIdIncrements() throws Exception {
        int firstGen = ddl().getGenerationAsInt(session());
        createTable("sch", "c1", "id int not null primary key");
        int secondGen = ddl().getGenerationAsInt(session());
        assertTrue(String.format("failed %d > %d", secondGen, firstGen), secondGen > firstGen);
    }

    @Test
    public void truncate() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);
        dml().truncateTable(session(), tableId);
        expectRowCount(tableId, 0);
    }

    /**
     * bug1002359: Grouped tables, different schemas, same table name, same column name
     */
    @Test
    public void groupedTablesWithSameNameAndColumnNames() {
        createTable("s1", "t1", "id int not null primary key");
        createTable("s2", "t1", "some_id int not null primary key, id int, grouping foreign key(id) references s1.t1(id)");
        createTable("s3", "t1", "some_id int not null primary key, id int, grouping foreign key(id) references s2.t1(some_id)");
        AkibanInformationSchema ais = ddl().getAIS(session());
        Group group = ais.getGroup(new TableName("s1", "t1"));
        assertNotNull("Found group", group);
        List<TableName> tablesInGroup = new ArrayList<>();
        for(Table table : ais.getTables().values()) {
            if(table.getGroup() == group) {
                tablesInGroup.add(table.getName());
            }
        }
        assertEquals("Tables in group", "[s1.t1, s2.t1, s3.t1]", tablesInGroup.toString());
    }

    private static class NoRowsUpdatedException extends RuntimeException {
    }
}
