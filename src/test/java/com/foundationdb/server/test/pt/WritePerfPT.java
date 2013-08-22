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

package com.foundationdb.server.test.pt;

import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WritePerfPT extends PTBase {
    private static final int MILLION = 1000000;
    private static final int BULK_CALLS = 35;
    private static final int ROWS_PER_BULK = MILLION / BULK_CALLS;

    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private RowDef createOneColumnTable() {
        int tid = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY");
        return getRowDef(tid);
    }

    private RowDef createTwoColumnTable() {
        int tid = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        return getRowDef(tid);
    }

    @Test
    public void writeMillionSingleRows() {
        RowDef rowDef = createOneColumnTable();
        RowData rowData = new RowData(new byte[100]);
        LegacyRowWrapper wrapper = new LegacyRowWrapper(null, rowData);
        for(int i = 0; i < MILLION; ++i) {
            rowData.createRow(rowDef, new Object[] { i });
            dml().writeRow(session(), wrapper);
        }
    }

    // Simulate a WriteRowRequest from the adapter that had the write cache enabled
    @Test
    public void writeMillionBulkRows() {
        RowDef rowDef = createOneColumnTable();
        List<RowData> rows = new ArrayList<>(ROWS_PER_BULK);
        for(int i = 0; i < ROWS_PER_BULK; ++i) {
            rows.add(new RowData(new byte[100]));
        }

        int rowID = 0;
        for(int bulkCall = 0; bulkCall < BULK_CALLS; ++bulkCall) {
            for(int listIndex = 0; listIndex < ROWS_PER_BULK; ++listIndex) {
                rows.get(listIndex).createRow(rowDef, new Object[] { rowID++ });
            }
            dml().writeRows(session(), rows);
        }
    }

    @Test
    public void updateMillionNonPkCols() {
        RowDef rowDef = createTwoColumnTable();
        RowData rowData1 = new RowData(new byte[100]);
        RowData rowData2 = new RowData(new byte[100]);
        rowData1.createRow(rowDef, new Object[] { 0, 0 });
        rowData2.createRow(rowDef, new Object[] { 0, 0 });
        LegacyRowWrapper wrapper1 = new LegacyRowWrapper(rowDef, rowData1);
        LegacyRowWrapper wrapper2 = new LegacyRowWrapper(rowDef, rowData2);

        for(int i = 0; i < MILLION; ++i) {
            rowData1.createRow(rowDef, new Object[] { i, i });
            dml().writeRow(session(), wrapper1);
        }

        long start = System.currentTimeMillis();
        for(int i = 0; i < MILLION; ++i) {
            rowData1.createRow(rowDef, new Object[] { i, i });
            rowData2.createRow(rowDef, new Object[] { i, i+10 });
            dml().updateRow(session(), wrapper1, wrapper2, null);
        }
        long end = System.currentTimeMillis();
        System.out.printf("Update elapsed: %dms\n",  end - start);
    }
}
