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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.api.FixedCountLimit;
import com.foundationdb.server.api.dml.scan.BufferFullException;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.LegacyRowOutput;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanFlag;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.api.dml.scan.WrappingRowOutput;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.GrowableByteBuffer;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public final class ScanBufferTooSmallIT extends ITBase {

    @Before
    public void createTables() throws InvalidOperationException {
        int cid = createTable("ts", "c",
                "cid int not null primary key",
                "name varchar(255)");
        int oid = createTable("ts", "o",
                "oid int not null primary key",
                "cid int",
                "GROUPING FOREIGN KEY (cid) REFERENCES c(cid)");
        int iid = createTable("ts", "i",
                "iid int not null primary key",
                "oid int",
                "GROUPING FOREIGN KEY (oid) REFERENCES o(oid)");

        writeRows(
                createNewRow(cid, 1, "short name"),
                createNewRow(oid, 1, 1),
                createNewRow(iid, 1, 1),
                createNewRow(iid, 2, 1),

                createNewRow(cid, 2, "this name is much longer than the previous name, which was short")
        );
    }

    @Test(expected=BufferFullException.class)
    public void onTable() throws InvalidOperationException, BufferFullException {
        Table table = getTable("ts", "c");
        doTest(table, table.getPrimaryKey().getIndex().getIndexId());
    }

    private void doTest(Table table, int indexId) throws InvalidOperationException, BufferFullException {
        Set<Integer> columns = allColumns(table);
        int size = sizeForOneRow(table.getTableId(), indexId, columns);
        LegacyRowOutput tooSmallOutput = new WrappingRowOutput( new GrowableByteBuffer(size) );
        tooSmallOutput.getOutputBuffer().mark();

        ScanRequest request = new ScanAllRequest(
                table.getTableId(), columns, indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END)
        );
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        try {
            dml().scanSome(session(), cursorId, tooSmallOutput);
        } finally {
            dml().closeCursor(session(), cursorId);
        }
    }

    private Set<Integer> allColumns(Table table) {
        Set<Integer> cols = new HashSet<>();
        int colsCount = table.getColumns().size();
        while (--colsCount >= 0) {
            cols.add(colsCount);
        }
        return cols;
    }

    private int sizeForOneRow(int tableId, int indexId, Set<Integer> columns) throws InvalidOperationException {
        LegacyRowOutput output = new WrappingRowOutput( new GrowableByteBuffer(1024) ); // plenty of space!
        output.getOutputBuffer().mark();

        ScanRequest request = new ScanAllRequest(
                tableId, columns, indexId,
                EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END),
                new FixedCountLimit(1)
        );
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        try {
            dml().scanSome(session(), cursorId, output);
        } catch (BufferFullException e) {
            throw new RuntimeException(e);
        } finally {
            dml().closeCursor(session(), cursorId);
        }

        return output.getOutputBuffer().position();
    }
}
