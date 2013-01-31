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

package com.akiban.server.test.pt;

import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
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
        List<RowData> rows = new ArrayList<RowData>(ROWS_PER_BULK);
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
