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

import com.foundationdb.server.api.FixedCountLimit;
import com.foundationdb.server.api.dml.scan.ColumnSet;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Inspired by bug 885697

public final class StaleScanDataIT extends ITBase
{
    @Test
    public void simpleScanLimit() throws InvalidOperationException
    {
        int t1 = createTable("schema", "t1",
                             "c1 int",
                             "c2 int",
                             "c3 int",
                             "id int not null primary key");
        int t2 = createTable("schema", "t2",
                             "id int not null primary key",
                             "c1 int");
        // Load some data
        dml().writeRow(session(), createNewRow(t1, 0, 0, 0, 0));
        dml().writeRow(session(), createNewRow(t1, 1, 1, 1, 1));
        dml().writeRow(session(), createNewRow(t2, 2, 2));
        dml().writeRow(session(), createNewRow(t2, 3, 3));
        // Start a scan on t1. Should leave a ScanData hanging around.
        ScanRequest t1ScanRequest = new ScanAllRequest(t1,
                                                       ColumnSet.ofPositions(0, 1, 2, 3),
                                                       0,
                                                       null,
                                                       new FixedCountLimit(1));
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), t1ScanRequest);
        assertEquals(1, dml().getCursors(session()).size());
        // Update a t2 row. This provokes bug 885697 because:
        // - The rows passed in are LegacyRowWrappers (as opposed to NiceRows). Indexing into the RowData's RowDef
        //   with a too-high field number results in ArrayIndexOutOfBoundsException.
        // - ColumnSelector is null (as in an UpdateRowRequest), so that we get past the ColumnSelector check in
        //   BasicDMLFunctions.checkForModifiedCursors to retrieve a field from the old/new rows.
        // - There is a non-closed ScanData whose index contains columns from field positions that don't exist
        //   in the old/new rows, (set up using the t1 scan).
        dml().updateRow(session(),
                        dml().wrapRowData(session(), createNewRow(t2, 2, 2).toRowData()),
                        dml().wrapRowData(session(), createNewRow(t2, 2, 999).toRowData()),
                        null);
        dml().closeCursor(session(), cursorId);
        assertTrue(dml().getCursors(session()).isEmpty());
    }
}
