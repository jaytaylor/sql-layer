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
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.LegacyScanRequest;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.store.RowCollector;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ScanFlagsIT extends ITBase
{
    @Test
    public void testFullScanAsc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DEFAULT, null, null,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 0, 1, 2, 3, 4);
    }

    @Test
    public void testFullScanDesc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DESCENDING, null, null,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 4, 3, 2, 1, 0);
    }

    @Test
    public void testGTAsc() throws InvalidOperationException
    {
        List<NewRow> actual = query(START_EXCLUSIVE, 2, null,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 3, 4);
    }

    @Test
    public void testGTDesc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DESCENDING | START_EXCLUSIVE, 2, null, 0, 1, 2, 3, 4);
        checkOutput(actual, 4, 3);
    }

    @Test
    public void testGEAsc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DEFAULT, 2, null,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 2, 3, 4);
    }

    @Test
    public void testGEDesc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DESCENDING, 2, null,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 4, 3, 2);
    }

    @Test
    public void testLTAsc() throws InvalidOperationException
    {
        List<NewRow> actual = query(END_EXCLUSIVE, null, 2,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 0, 1);
    }

    @Test
    public void testLTDesc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DESCENDING | END_EXCLUSIVE, null, 2,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 1, 0);
    }

    @Test
    public void testLEAsc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DEFAULT, null, 2,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 0, 1, 2);
    }

    @Test
    public void testLEDesc() throws InvalidOperationException
    {
        List<NewRow> actual = query(DESCENDING, null, 2,
                                    0, 1, 2, 3, 4);
        checkOutput(actual, 2, 1, 0);
    }

    private List<NewRow> query(int flags, Integer start, Integer end, int ... values) throws InvalidOperationException
    {
        rowDefId = createTable("schema", "t", "id int not null primary key, idcopy int");
        createIndex("schema", "t", "idcopy", "idcopy");
        Table table = super.getTable(rowDefId);
        Index idCopyIndex = null;
        for (Index index : table.getIndexes()) {
            if (!index.isPrimaryKey()) {
                idCopyIndex = index;
            }
        }
        assertNotNull(idCopyIndex);
        for (int x : values) {
            writeRow(rowDefId, x, x);
        }
        flags |= RowCollector.SCAN_FLAGS_LEXICOGRAPHIC;
        LegacyScanRequest request = new LegacyScanRequest(rowDefId,
                                                          bound(start),
                                                          null,
                                                          bound(end),
                                                          null,
                                                          new byte[]{1},
                                                          idCopyIndex.getIndexId(),
                                                          flags,  // scan flags
                                                          ScanLimit.NONE);
        ListRowOutput output = new ListRowOutput();
        CursorId cursorId = dml().openCursor(session(), aisGeneration(), request);
        dml().scanSome(session(), cursorId, output);
        dml().closeCursor(session(), cursorId);
        return output.getRows();
    }

    private void checkOutput(List<NewRow> actual, int... expected)
    {
        assertEquals(expected.length, actual.size());
        Iterator<NewRow> a = actual.iterator();
        for (int e : expected) {
            assertEquals((long) e, ((Integer)a.next().get(0)).longValue());
        }
    }

    private RowData bound(Integer x)
    {
        RowData rowData = null;
        if (x != null) {
            rowData = new RowData(new byte[100]);
            RowDef rowDef = getRowDef(rowDefId);
            rowData.createRow(rowDef, new Object[]{null, x});
        }
        return rowData;
    }

    // From message compendium
    private static final int DEFAULT = 0x0;
    private static final int DESCENDING = 0x1;
    private static final int START_EXCLUSIVE = 0x2;
    private static final int END_EXCLUSIVE = 0x4;
    private static final int SINGLE_ROW = 0x8;
    private static final int PREFIX = 0x10;
    private static final int START_AT_EDGE = 0x20;
    private static final int END_AT_EDGE = 0x40;
    private static final int DEEP = 0x80;

    private int rowDefId;
}
