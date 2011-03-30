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

package com.akiban.server.itests.keyupdate;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.EasyUseColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.itests.ApiTestBase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class BasicKeyUpdateIT extends ApiTestBase {
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
                Arrays.asList(
                        createNewRow(tableId, 2L, "a")
                ),
                Arrays.asList(
                        createNewRow(tableId, 2L, "a")
                )
        );
    }

    @Test// @Ignore("bug 746006")
    public void oldKeysAreRemoved_2Rows_IndexChanged() throws InvalidOperationException {
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
                        List<NewRow> endRows,
                        List<NewRow> endRowsByName

    ) throws InvalidOperationException
    {

        if (!initialRows.contains( createNewRow(tableId, 2L, "c"))) {
            throw new RuntimeException("required row not found");
        }
        if (initialRows.size() != endRows.size()) {
            throw new RuntimeException("initial and end row lists must be of equal size");
        }

        writeRows( initialRows.toArray(new NewRow[initialRows.size()]) );
        expectRows(byNameScan(tableId), initialRowsByName.toArray(new NewRow[initialRowsByName.size()]));

        NewRow oldMr = new NiceRow(tableId);
        oldMr.put(0, 2L);
        NewRow updatedMr = new NiceRow(tableId);
        updatedMr.put(1, "a");

        dml().updateRow(session(), oldMr, updatedMr, new EasyUseColumnSelector(1));

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
        return createTable(SCHEMA, TABLE, "id int key", "name varchar(32)", "key(name)");
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
