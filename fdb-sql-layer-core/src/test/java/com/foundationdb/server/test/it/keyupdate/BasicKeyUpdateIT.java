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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class BasicKeyUpdateIT extends ITBase {
    protected static final String SCHEMA = "cold";
    protected static final String TABLE = "frosty";

    @Test
    public void oldKeysAreRemoved_1Row() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2)
                )
        );
    }

    @Test
    @Ignore
    public void oldKeysAreRemoved_2Rows_Partial_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, null),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexChanged() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexAndPKMovesBackward() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 0, "a"),

                Arrays.asList(
                        row(tableId, 0, "a"),
                        row(tableId, 1, "b")
                ),
                Arrays.asList(
                        row(index, "a", 0),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexAndPKMovesForward() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "b", 1),
                        row(index, "c", 2)
                ),

                row(tableId, 2, "c"),
                row(tableId, 3, "a"),

                Arrays.asList(
                        row(tableId, 1, "b"),
                        row(tableId, 3, "a")
                ),
                Arrays.asList(
                        row(index, "a", 3),
                        row(index, "b", 1)
                )
        );
    }

    @Test
    public void oldKeysAreRemoved_2Rows_IndexSame() throws InvalidOperationException {
        int tableId = table();
        Index index = nameIndex();
        runTest(
                tableId,
                Arrays.asList(
                        row(tableId, 1, "d"),
                        row(tableId, 2, "c")
                ),
                Arrays.asList(
                        row(index, "c", 2),
                        row(index, "d", 1)
                ),

                row(tableId, 2, "c"),
                row(tableId, 2, "a"),

                Arrays.asList(
                        row(tableId, 1, "d"),
                        row(tableId, 2, "a")
                ),
                Arrays.asList(
                        row(index, "a", 2),
                        row(index, "d", 1)
                )
        );
    }

    public void runTest(int tableId,
                        List<Row> initialRows,
                        List<Row> initialRowsByName,
                        Row rowToUpdate,
                        Row updatedValue,
                        List<Row> endRows,
                        List<Row> endRowsByName

    ) throws InvalidOperationException
    {
        Set<Integer> sizes = new HashSet<>();
        sizes.add( initialRows.size() );
        sizes.add( initialRowsByName.size() );
        sizes.add( endRows.size() );
        sizes.add( endRowsByName.size() );
        if(sizes.size() != 1) {
            throw new RuntimeException("All lists must be of the same size");
        }

        writeRows( initialRows.toArray(new Row[initialRows.size()]) );
        expectRows(
            nameIndex(),
            initialRowsByName
        );

        updateRow(rowToUpdate, updatedValue);

        expectRows(
                tableId,
                endRows.toArray(new Row[endRows.size()])
        );

        expectRows(
            nameIndex(),
            endRowsByName
        );
    }

    private int table() throws InvalidOperationException {
        int tid = createTable(SCHEMA, TABLE, "id int not null primary key", "name varchar(32)");
        createIndex(SCHEMA, TABLE, "name", "name");
        return tid;
    }

    private Index nameIndex() {
        return getTable(SCHEMA, TABLE).getIndex("name");
    }
}
