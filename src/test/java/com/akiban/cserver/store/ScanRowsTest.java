package com.akiban.cserver.store;

import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_DESCENDING;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_END_AT_EDGE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_END_EXCLUSIVE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_PREFIX;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_SINGLE_ROW;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_START_AT_EDGE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_START_EXCLUSIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.PrintWriter;

import org.junit.Test;

import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.persistit.Exchange;

public class ScanRowsTest extends AbstractScanBase {

    @Test
    public void testScanRows() throws Exception {
        final RowDef rowDef = groupRowDef("_akiba_a");
        final int fc = rowDef.getFieldCount();
        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        byte[] bitMap;

        {
            // Just the root table rows
            final RowDef userRowDef = userRowDef("a");
            start.createRow(rowDef, new Object[fc]);
            end.createRow(rowDef, new Object[fc]);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(10, scanAllRows("all a", start, end, bitMap, 0));
        }

        {
            final RowDef userRowDef = userRowDef("aaaa");
            start.createRow(rowDef, new Object[fc]);
            end.createRow(rowDef, new Object[fc]);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(11110, scanAllRows("all aaaa", start, end, bitMap,
                    findIndexId(rowDef, userRowDef, 1)));
        }

        {
            final RowDef userRowDef = userRowDef("aaaa");
            int pkcol = columnOffset(userRowDef, rowDef);
            assertTrue(pkcol >= 0);
            pkcol += userRowDef.getPkFields()[0];
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[pkcol] = 1;
            endValue[pkcol] = 2;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(5, scanAllRows("aaaa with aaaa.aaaa1 in [1,2]", start,
                    end, bitMap, findIndexId(rowDef, userRowDef, 1)));
        }

        {
            final RowDef userRowDef = userRowDef("aaaa");
            int pkcol = columnOffset(userRowDef, rowDef);
            assertTrue(pkcol >= 0);
            pkcol += userRowDef.getPkFields()[0];
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[pkcol] = 100;
            endValue[pkcol] = 200;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(115, scanAllRows("aaaa with aaaa.aaaa1 in [100,200]",
                    start, end, bitMap, findIndexId(rowDef, userRowDef, 1)));
        }

        {
            final RowDef userRowDef = userRowDef("aaaa");
            int col = findFieldIndex(rowDef, "aa$aa1");
            int indexId = findIndexId(rowDef, rowDef.getTableName() + "$PK_" + userRowDef.getRowDefId());
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = 1;
            endValue[col] = 5;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(556, scanAllRows("aaaa with aa.aa1 in [1,5]", start,
                    end, bitMap, indexId));
        }
        
        store.deleteIndexes("");
        
        {
            final RowDef userRowDef = userRowDef("aaaa");
            int col = findFieldIndex(rowDef, "aa$aa1");
            int indexId = findIndexId(rowDef, rowDef.getTableName() + "$aa_PK");
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = 1;
            endValue[col] = 5;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(0, scanAllRows("aaaa with aa.aa1 in [1,5]", start,
                    end, bitMap, indexId));
        }
        
        store.buildIndexes("");

        {
            final RowDef userRowDef = userRowDef("aaaa");
            int col = findFieldIndex(rowDef, "aa$aa1");
            int indexId = findIndexId(rowDef, rowDef.getTableName() + "$aa_PK");
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = 1;
            endValue[col] = 5;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(556, scanAllRows("aaaa with aa.aa1 in [1,5]", start,
                    end, bitMap, indexId));
        }
    }

    @Test
    public void testScanFlags() throws Exception {
        final RowDef rowDef = groupRowDef("_akiba_a");
        final int fc = rowDef.getFieldCount();
        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        byte[] bitMap;

        {
            final RowDef userRowDef = userRowDef("a");
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE, null,
                    null, bitMap, 0));
            assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                            | SCAN_FLAGS_START_EXCLUSIVE
                            | SCAN_FLAGS_END_EXCLUSIVE, null, null, bitMap, 0));
            assertEquals(1, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                            | SCAN_FLAGS_START_EXCLUSIVE
                            | SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_SINGLE_ROW,
                    null, null, bitMap, 0));
            assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                            | SCAN_FLAGS_START_EXCLUSIVE
                            | SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_DESCENDING,
                    null, null, bitMap, 0));
            final RowData nullRow = new RowData(new byte[256]);
            nullRow.createRow(userRowDef, new Object[0]);
            assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE 
                            | SCAN_FLAGS_START_EXCLUSIVE
                            | SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_DESCENDING,
                    null, nullRow, bitMap, 0));
            assertEquals(10, scanAllRows("all a", userRowDef.getRowDefId(),
                    SCAN_FLAGS_END_AT_EDGE 
                            | SCAN_FLAGS_START_EXCLUSIVE
                            | SCAN_FLAGS_END_EXCLUSIVE | SCAN_FLAGS_DESCENDING,
                    nullRow, null, bitMap, 0));
            
        }

        {
            final RowDef userRowDef = userRowDef("aaaa");
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(11110, scanAllRows("all aaaa", rowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE, null,
                    null, bitMap, findIndexId(rowDef, userRowDef, 1)));
            assertEquals(4, scanAllRows("all aaaa", rowDef.getRowDefId(),
                    SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                            | SCAN_FLAGS_SINGLE_ROW, null, null, bitMap,
                    findIndexId(rowDef, userRowDef, 1)));
        }
        {
            final RowDef userRowDef = userRowDef("aa");
            int col = findFieldIndex(rowDef, "aa$aa4");
            int indexId = findIndexId(rowDef, "aa$str");
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = "1";
            endValue[col] = "2";
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), 0, start, end, bitMap, indexId));
            assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_START_EXCLUSIVE, start,
                    end, bitMap, indexId));
            assertEquals(13, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE, start, end,
                    bitMap, indexId));
            assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE
                            | SCAN_FLAGS_SINGLE_ROW, start, end, bitMap,
                    indexId));
            assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_SINGLE_ROW
                            | SCAN_FLAGS_DESCENDING, start, end, bitMap,
                    indexId));
            assertEquals(2, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_SINGLE_ROW
                            | SCAN_FLAGS_DESCENDING | SCAN_FLAGS_PREFIX, start,
                    end, bitMap, indexId));
            assertEquals(26, scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                    rowDef.getRowDefId(), SCAN_FLAGS_PREFIX, start, end,
                    bitMap, indexId));
        }
    }

    @Test
    public void testCoveringIndex() throws Exception {
        final RowDef rowDef = userRowDef("aaab");
        final int indexId = findIndexId(rowDef, "aaab3aaab1");
        final byte[] columnBitMap = new byte[1];
        columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab1");
        columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab3");
        final int count = scanAllRows("Covering index aaab3aaab1", rowDef
                .getRowDefId(), SCAN_FLAGS_START_AT_EDGE
                | SCAN_FLAGS_END_AT_EDGE, null, null, columnBitMap, indexId);
        assertTrue(count > 0);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final RowData rowData : result) {
            sb.append(rowData.toString(rowDefCache));
            sb.append(CServerUtil.NEW_LINE);
        }
        return sb.toString();
    }

    public static void main(final String[] args) throws Exception {
        final ScanRowsTest srt = new ScanRowsTest();
        srt.scanLongTime();
    }

    /**
     * Infinite loop while scanning rows - useful for profiling
     * 
     * @throws Exception
     */
    private void scanLongTime() throws Exception {
        setUpSuite();

        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        final RowDef rowDef = userRowDef("aaaa");
        start.createRow(rowDef, new Object[] { null, 123 });
        end.createRow(rowDef, new Object[] { null, 132 });
        final int indexId = findIndexId(rowDef, rowDef, 1);
        byte[] bitMap = new byte[] { 0xF };
        System.out.println("Starting SELECT loop");
        long time = System.nanoTime();
        long iterations = 0;
        for (;;) {
            assertEquals(10, scanAllRows("one aaaa", start, end, bitMap,
                    indexId));
            iterations++;
            if (iterations % 100 == 0) {
                long newtime = System.nanoTime();
                if (newtime - time > 5000000000L) {
                    System.out.println(String.format("%10dns per iteration",
                            (newtime - time) / iterations));
                    iterations = 1;
                    time = newtime;
                }
            }
        }
    }
}
