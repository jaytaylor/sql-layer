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

package com.akiban.server.test.it.rowtests;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class RowTestIT extends ITBase
{
    @Test
    public void rowConversionTestNoNulls() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int not null",
                                "b int not null");
        NiceRow original = new NiceRow(t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
        original.put(cB, 300L);
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void rowConversionTestWithNulls() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int not null",
                                "b int");
        NiceRow original = new NiceRow(t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
        original.put(cB, null);
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void niceRowUpdate() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow row = new NiceRow(t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        row.put(cId, 100L);
        row.put(cA, 200L);
        row.put(cB, 300L);
        row.put(cC, null);
        assertEquals(100L, row.get(cId));
        assertEquals(200L, row.get(cA));
        assertEquals(300L, row.get(cB));
        assertNull(row.get(cC));
        row.put(cA, 222L);
        row.put(cB, null);
        row.put(cC, 444L);
        assertEquals(100L, row.get(cId));
        assertEquals(222L, row.get(cA));
        assertNull(row.get(cB));
        assertEquals(444L, row.get(cC));
    }

    @Test
    public void rowDataUpdate() throws InvalidOperationException
    {
        int t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 100L);
        niceRow.put(cA, 200L);
        niceRow.put(cB, 300L);
        niceRow.put(cC, null);
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(niceRow.toRowData(), store());
        assertEquals(100L, legacyRow.get(cId));
        assertEquals(200L, legacyRow.get(cA));
        assertEquals(300L, legacyRow.get(cB));
        assertNull(legacyRow.get(cC));
        legacyRow.put(cA, 222L);
        legacyRow.put(cB, null);
        legacyRow.put(cC, 444L);
        assertEquals(100L, legacyRow.get(cId));
        assertEquals(222L, legacyRow.get(cA));
        assertNull(legacyRow.get(cB));
        assertEquals(444L, legacyRow.get(cC));
    }

    @Test
    public void legacyRowConversion() throws InvalidOperationException
    {
        // LegacyRowWrapper converts to NiceRow on update, back to RowData on toRowData().
        // Check the conversions.
        int t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 0);
        niceRow.put(cA, 0L);
        niceRow.put(cB, 0L);
        niceRow.put(cC, 0L);
        // Create initial legacy row
        LegacyRowWrapper legacyRow = new LegacyRowWrapper((niceRow.toRowData()), store());
        assertEquals(0L, legacyRow.get(cA));
        assertEquals(0L, legacyRow.get(cB));
        assertEquals(0L, legacyRow.get(cC));
        // Apply a few updates
        legacyRow.put(cA, 1L);
        legacyRow.put(cB, 1L);
        legacyRow.put(cC, 1L);
        // Check the updates (should be a NiceRow)
        assertEquals(1L, legacyRow.get(cA));
        assertEquals(1L, legacyRow.get(cB));
        assertEquals(1L, legacyRow.get(cC));
        // Convert to LegacyRow and check NiceRow created from the legacy row's RowData
        RowDef rowDef = rowDefCache().getRowDef(t);
        niceRow = (NiceRow) NiceRow.fromRowData(legacyRow.toRowData(), rowDef);
        assertEquals(1L, niceRow.get(cA));
        assertEquals(1L, niceRow.get(cB));
        assertEquals(1L, niceRow.get(cC));
        // Convert back to NiceRow and check state again
        legacyRow.put(cA, 2L);
        legacyRow.put(cB, 2L);
        legacyRow.put(cC, 2L);
        assertEquals(2L, legacyRow.get(cA));
        assertEquals(2L, legacyRow.get(cB));
        assertEquals(2L, legacyRow.get(cC));
    }
}
