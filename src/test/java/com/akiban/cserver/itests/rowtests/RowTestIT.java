package com.akiban.cserver.itests.rowtests;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.LegacyRowWrapper;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class RowTestIT extends ApiTestBase
{
    @Test
    public void rowConversionTestNoNulls() throws InvalidOperationException
    {
        TableId t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int not null",
                                "b int not null");
        NiceRow original = new NiceRow(t);
        ColumnId cId = ColumnId.of(0);
        ColumnId cA = ColumnId.of(1);
        ColumnId cB = ColumnId.of(2);
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
        original.put(cB, 300L);
        RowDef rowDef = rowDefCache().getRowDef(t.getTableId(null));
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void rowConversionTestWithNulls() throws InvalidOperationException
    {
        TableId t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int not null",
                                "b int");
        NiceRow original = new NiceRow(t);
        ColumnId cId = ColumnId.of(0);
        ColumnId cA = ColumnId.of(1);
        ColumnId cB = ColumnId.of(2);
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
        original.put(cB, null);
        RowDef rowDef = rowDefCache().getRowDef(t.getTableId(null));
        RowData rowData = original.toRowData();
        NiceRow reconstituted = (NiceRow) NiceRow.fromRowData(rowData, rowDef);
        assertEquals(original, reconstituted);
    }

    @Test
    public void niceRowUpdate() throws InvalidOperationException
    {
        TableId t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow row = new NiceRow(t);
        ColumnId cId = ColumnId.of(0);
        ColumnId cA = ColumnId.of(1);
        ColumnId cB = ColumnId.of(2);
        ColumnId cC = ColumnId.of(3);
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
        TableId t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(t);
        ColumnId cId = ColumnId.of(0);
        ColumnId cA = ColumnId.of(1);
        ColumnId cB = ColumnId.of(2);
        ColumnId cC = ColumnId.of(3);
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 100L);
        niceRow.put(cA, 200L);
        niceRow.put(cB, 300L);
        niceRow.put(cC, null);
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(niceRow.toRowData());
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
        TableId t = createTable("s",
                                "t",
                                "id int not null key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(t);
        ColumnId cId = ColumnId.of(0);
        ColumnId cA = ColumnId.of(1);
        ColumnId cB = ColumnId.of(2);
        ColumnId cC = ColumnId.of(3);
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 0);
        niceRow.put(cA, 0L);
        niceRow.put(cB, 0L);
        niceRow.put(cC, 0L);
        // Create initial legacy row
        LegacyRowWrapper legacyRow = new LegacyRowWrapper((niceRow.toRowData()));
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
        RowDef rowDef = rowDefCache().getRowDef(t.getTableId(null));
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
