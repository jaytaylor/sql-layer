/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
                                "id int not null primary key",
                                "a int not null",
                                "b int not null");
        NiceRow original = new NiceRow(session(), t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
        original.put(cB, 300L);
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
        NiceRow original = new NiceRow(session(), t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        original.put(cId, 100L);
        original.put(cA, 200L);
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
        NiceRow row = new NiceRow(session(), t, store());
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
                                "id int not null primary key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(session(), t, store());
        int cId = 0;
        int cA = 1;
        int cB = 2;
        int cC = 3;
        // Insert longs, not integers, because Persistit stores all ints as 8-byte.
        niceRow.put(cId, 100L);
        niceRow.put(cA, 200L);
        niceRow.put(cB, 300L);
        niceRow.put(cC, null);
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(session(), niceRow.toRowData(), store());
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
                                "id int not null primary key",
                                "a int",
                                "b int",
                                "c int");
        NiceRow niceRow = new NiceRow(session(), t, store());
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
        LegacyRowWrapper legacyRow = new LegacyRowWrapper(session(), niceRow.toRowData(), store());
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
        RowDef rowDef = getRowDef(t);
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
