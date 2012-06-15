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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyScanRequest;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.store.RowCollector;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

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
        UserTable table = super.getUserTable(rowDefId);
        Index idCopyIndex = null;
        for (Index index : table.getIndexes()) {
            if (!index.isPrimaryKey()) {
                idCopyIndex = index;
            }
        }
        assertNotNull(idCopyIndex);
        for (int x : values) {
            dml().writeRow(session(), createNewRow(rowDefId, x, x));
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
            assertEquals((long) e, ((Long)a.next().get(0)).longValue());
        }
    }

    private RowData bound(Integer x)
    {
        RowData rowData = null;
        if (x != null) {
            rowData = new RowData(new byte[100]);
            RowDef rowDef = rowDefCache().rowDef(rowDefId);
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
