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

package com.foundationdb.server.test.it.rowtests;

import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RowTestIT extends ITBase
{
    @Test
    public void rowConversionTestNoNulls() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null primary key",
                                "a int not null",
                                "b int not null");
        NewRow original = createNewRow(t);
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100);
        original.put(cA, 200);
        original.put(cB, 300);
        RowDef rowDef = getRowDef(t);
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void rowConversionTestWithNulls() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null primary key",
                                "a int not null",
                                "b int");
        NewRow original = createNewRow(t);
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100);
        original.put(cA, 200);
        original.put(cB, null);
        RowDef rowDef = getRowDef(t);
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void niceRowUpdate() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null primary key",
                                "a int",
                                "b int",
                                "c int");
        NewRow row = createNewRow(t);
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        row.put(cId, 100);
        row.put(cA, 200);
        row.put(cB, 300);
        row.put(cC, null);
        assertEquals(100, row.get(cId));
        assertEquals(200, row.get(cA));
        assertEquals(300, row.get(cB));
        assertNull(row.get(cC));
        row.put(cA, 222);
        row.put(cB, null);
        row.put(cC, 444);
        assertEquals(100, row.get(cId));
        assertEquals(222, row.get(cA));
        assertNull(row.get(cB));
        assertEquals(444, row.get(cC));
    }

    @Test
    public void rowDataUpdate() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null primary key",
                                "a int",
                                "b int",
                                "c int");
        NewRow niceRow = createNewRow(t);
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 100);
        niceRow.put(cA, 200);
        niceRow.put(cB, 300);
        niceRow.put(cC, null);
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(niceRow.getRowDef(), niceRow.toRowData());
        assertEquals(100, legacyRow.get(cId));
        assertEquals(200, legacyRow.get(cA));
        assertEquals(300, legacyRow.get(cB));
        assertNull(legacyRow.get(cC));
        legacyRow.put(cA, 222);
        legacyRow.put(cB, null);
        legacyRow.put(cC, 444);
        assertEquals(100, legacyRow.get(cId));
        assertEquals(222, legacyRow.get(cA));
        assertNull(legacyRow.get(cB));
        assertEquals(444, legacyRow.get(cC));
    }

    @Test
    public void legacyRowConversion() throws InvalidOperationException
    {
        // LegacyRowWrapper converts to NiceRow on update, back to RowData on toRowData().
        // Check the conversions.
        int t = createTable("s",
                                "t",
                                "id int not null primary key",
                                "a int",
                                "b int",
                                "c int");
        NewRow niceRow = createNewRow(t);
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 0);
        niceRow.put(cA, 0);
        niceRow.put(cB, 0);
        niceRow.put(cC, 0);
        // Create initial legacy row
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(niceRow.getRowDef(), niceRow.toRowData());
        assertEquals(0, legacyRow.get(cA));
        assertEquals(0, legacyRow.get(cB));
        assertEquals(0, legacyRow.get(cC));
        // Apply a few updates
        legacyRow.put(cA, 1);
        legacyRow.put(cB, 1);
        legacyRow.put(cC, 1);
        // Check the updates (should be a NiceRow)
        assertEquals(1, legacyRow.get(cA));
        assertEquals(1, legacyRow.get(cB));
        assertEquals(1, legacyRow.get(cC));
        // Convert to LegacyRow and check NiceRow created from the legacy row's RowData
        RowDef rowDef = getRowDef(t);
        niceRow = (NiceRow) NiceRow.fromRowData(legacyRow.toRowData(), rowDef);
        assertEquals(1, niceRow.get(cA));
        assertEquals(1, niceRow.get(cB));
        assertEquals(1, niceRow.get(cC));
        // Convert back to NiceRow and check state again
        legacyRow.put(cA, 2);
        legacyRow.put(cB, 2);
        legacyRow.put(cC, 2);
        assertEquals(2, legacyRow.get(cA));
        assertEquals(2, legacyRow.get(cB));
        assertEquals(2, legacyRow.get(cC));
    }
}
