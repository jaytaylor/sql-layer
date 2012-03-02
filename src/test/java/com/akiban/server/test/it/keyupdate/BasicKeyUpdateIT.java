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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class BasicKeyUpdateIT extends ITBase {
    protected static final String SCHEMA = "cold";
    protected static final String TABLE = "frosty";

    @Test
    public void oldKeysAreRemoved_1Row() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "c")
                ),

                createNewRow(tableId, 2L, "c"),
                createNewRow(tableId, 2L, "a"),
                set(1),

                Arrays.asList(
                        createNewRow(tableId, 2L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "a")
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_Partial_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),

                createNewRow(tableId, 2L, UNDEF),
                createNewRow(tableId, 2L, "a"),
                set(1),

                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "a"),
                        createNewRow(tableId, 1L, "b")
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_Full_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),

                createNewRow(tableId, 2L, "c"),
                createNewRow(tableId, 2L, "a"),
                set(1),

                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "a"),
                        createNewRow(tableId, 1L, "b")
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_Partial_IndexAndPKMovesBackward() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),

                createNewRow(tableId, 2L, UNDEF),
                createNewRow(tableId, 0L, "a"),
                set(0, 1),

                Arrays.asList(
                        createNewRow(tableId, 0L, "a"),
                        createNewRow(tableId, 1L, "b")
                ),
                Arrays.asList(
                        createNewRow(tableId, 0L, "a"),
                        createNewRow(tableId, 1L, "b")
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_Partial_IndexAndPKMovesForward() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 2L, "c")
                ),

                createNewRow(tableId, 2L, UNDEF),
                createNewRow(tableId, 3L, "a"),
                set(0, 1),

                Arrays.asList(
                        createNewRow(tableId, 1L, "b"),
                        createNewRow(tableId, 3L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 3L, "a"),
                        createNewRow(tableId, 1L, "b")
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexSame() throws InvalidOperationException {
        int tableId = table();
        runTest(
                tableId,
                Arrays.asList(
                        createNewRow(tableId, 1L, "d"),
                        createNewRow(tableId, 2L, "c")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "c"),
                        createNewRow(tableId, 1L, "d")
                ),

                createNewRow(tableId, 2L, "c"),
                createNewRow(tableId, 2L, "a"),
                set(1),

                Arrays.asList(
                        createNewRow(tableId, 1L, "d"),
                        createNewRow(tableId, 2L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "a"),
                        createNewRow(tableId, 1L, "d")
                )
        );
    }

    public void runTest(int tableId,
                        List<NewRow> initialRows,
                        List<NewRow> initialRowsByName,
                        NewRow rowToUpdate,
                        NewRow updatedValue,
                        Set<Integer> fieldsToUpdate,
                        List<NewRow> endRows,
                        List<NewRow> endRowsByName

    ) throws InvalidOperationException
    {
        Set<Integer> sizes = new HashSet<Integer>();
        sizes.add( initialRows.size() );
        sizes.add( initialRowsByName.size() );
        sizes.add( endRows.size() );
        sizes.add( endRowsByName.size() );
        if(sizes.size() != 1) {
            throw new RuntimeException("All lists must be of the same size");
        }

        writeRows( initialRows.toArray(new NewRow[initialRows.size()]) );
        expectRows(byNameScan(tableId), initialRowsByName.toArray(new NewRow[initialRowsByName.size()]));

        dml().updateRow(session(), rowToUpdate, updatedValue, new SetColumnSelector(fieldsToUpdate));

        expectFullRows(
                tableId,
                endRows.toArray(new NewRow[endRows.size()])
        );

        List<NewRow> scanAllResult = scanAll(byNameScan(tableId));
        assertEquals("scan size: " + scanAllResult, endRows.size(), scanAllResult.size());
        assertEquals(
                "scan values",
                endRowsByName,
                scanAllResult
        );
    }

    private int table() throws InvalidOperationException {
        int tid = createTable(SCHEMA, TABLE, "id int not null primary key", "name varchar(32)");
        createIndex(SCHEMA, TABLE, "name", "name");
        return tid;
    }

    private ScanRequest byNameScan(int tableId) throws NoSuchTableException {
        int indexId = ddl().getUserTable(session(), tableName(SCHEMA, TABLE)).getIndex("name").getIndexId();
        return new ScanAllRequest(
                tableId, set(0, 1), indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                ScanLimit.NONE
        );
    }
}
