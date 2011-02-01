/*
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

package com.akiban.cserver.itests.bugs.bug687225;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.GenericInvalidOperationException;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.ScanAllRequest;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.cserver.store.RowCollector;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public final class TruncateTableIT extends ApiTestBase {
    @Test
    public void basicTruncate() throws InvalidOperationException {
        int tableId = createTable("test", "t", "id int key");

        expectRowCount(tableId, 0);
        final int rowCount = 5;
        for(int i = 1; i <= rowCount; ++i) {
            dml().writeRow(session, createNewRow(tableId, i));
        }
        expectRowCount(tableId, rowCount);

        dml().truncateTable(session, tableId);

        // Check that we can still get the rows
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null));
        assertEquals("Rows scanned", 0, rows.size());
    }
    
    @Test(expected=GenericInvalidOperationException.class)
    public void bugTestCase() throws InvalidOperationException {
        int tableId = createTable("test", "t", "id int NOT NULL, pid int NOT NULL, PRIMARY KEY(id), UNIQUE KEY pid(pid)");

        expectRowCount(tableId, 0);
        dml().writeRow(session, createNewRow(tableId, 1, 1));
        dml().writeRow(session, createNewRow(tableId, 2, 2));
        expectRowCount(tableId, 2);

        dml().truncateTable(session, tableId);
        
        int indexId = ddl().getAIS(session).getTable("test", "t").getIndex("pid").getIndexId();
        int scanFlags = RowCollector.SCAN_FLAGS_START_AT_EDGE | RowCollector.SCAN_FLAGS_END_AT_EDGE;
        
        // Exception currently thrown during dml.doScan: Corrupt RowData at {1,(long)1}
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null, indexId, scanFlags));
        assertEquals("Rows scanned", 0, rows.size());
    }
}
