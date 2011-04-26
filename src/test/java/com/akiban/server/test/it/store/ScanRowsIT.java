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

package com.akiban.server.test.it.store;

import static com.akiban.server.store.RowCollector.SCAN_FLAGS_DESCENDING;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_END_AT_EDGE;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_END_EXCLUSIVE;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_PREFIX;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_SINGLE_ROW;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_START_AT_EDGE;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_START_EXCLUSIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import com.akiban.server.store.RowCollector;
import org.junit.Test;

import com.akiban.server.RowData;
import com.akiban.server.RowDef;

public class ScanRowsIT extends AbstractScanBase {
    private static final boolean BUILD_INDEXES_DEFERRED = true;

    @Test
    public void testScanRows() throws Exception {
        final RowDef rowDef = rowDef("_akiban_a");
        final int fc = rowDef.getFieldCount();
        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        byte[] bitMap;

        // Test querying join columns within a group table, on both sides of the join
        // parent side
        {
            final RowDef userRowDef = rowDef("aaa");
            RowDef parentRowDef = rowDefCache.getRowDef(userRowDef.getParentRowDefId());
            int pkcol = findFieldIndex(rowDef, "aa$aa1");
            assertTrue(pkcol >= 0);
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[pkcol] = 1;
            endValue[pkcol] = 1;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    12, // 10 aaa, 1 aa, 1 a
                    scanAllRows("aaa with aaa$aa1 = 1 (restrict parent side of fk)", start, end,
                            bitMap, findIndexId(rowDef, parentRowDef, 1))); // position of aa1 in the aa table
        }
        // child side
        {
            final RowDef userRowDef = rowDef("aaa");
            int fkcol = findFieldIndex(rowDef, "aaa$aa1");
            assertTrue(fkcol >= 0);
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[fkcol] = 1;
            endValue[fkcol] = 1;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    12, // 10 aaa, 1 aa, 1 a
                    scanAllRows("aaa with aaa$aa1 = 1 (restrict child side of fk)", start, end,
                            bitMap, findIndexId(rowDef, userRowDef, 0))); // position of aa1 in the aaa table
        }

        store.deleteIndexes(session, "");

        {
            final RowDef userRowDef = rowDef("aaaa");
            int col = findFieldIndex(rowDef, "aa$aa1");
            int indexId = rowDef("aa").getPKIndexDef().getId();
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = 1;
            endValue[col] = 5;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    0,
                    scanAllRows("aaaa with aa.aa1 in [1,5]", start, end,
                            bitMap, indexId));
        }

        store.buildIndexes(session, "", BUILD_INDEXES_DEFERRED);

        {
            final RowDef userRowDef = rowDef("aaaa");
            int col = findFieldIndex(rowDef, "aa$aa1");
            int indexId = rowDef("aa").getPKIndexDef().getId();
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = 1;
            endValue[col] = 5;
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    556,
                    scanAllRows("aaaa with aa.aa1 in [1,5]", start, end,
                            bitMap, indexId));
        }
    }

    @Test
    public void testScanFlags() throws Exception {
        final RowDef rowDef = rowDef("_akiban_a");
        final int fc = rowDef.getFieldCount();
        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        byte[] bitMap;

        {
            final RowDef userRowDef = rowDef("a");
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    10,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE,
                            null, null, bitMap, 0));
            assertEquals(
                    10,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                                    | SCAN_FLAGS_START_EXCLUSIVE
                                    | SCAN_FLAGS_END_EXCLUSIVE, null, null,
                            bitMap, 0));
            assertEquals(
                    1,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                                    | SCAN_FLAGS_START_EXCLUSIVE
                                    | SCAN_FLAGS_END_EXCLUSIVE
                                    | SCAN_FLAGS_SINGLE_ROW, null, null,
                            bitMap, 0));
            assertEquals(
                    10,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                                    | SCAN_FLAGS_START_EXCLUSIVE
                                    | SCAN_FLAGS_END_EXCLUSIVE
                                    | SCAN_FLAGS_DESCENDING, null, null,
                            bitMap, 0));
            final RowData nullRow = new RowData(new byte[256]);
            nullRow.createRow(userRowDef, new Object[0]);
            assertEquals(
                    10,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE
                                    | SCAN_FLAGS_START_EXCLUSIVE
                                    | SCAN_FLAGS_END_EXCLUSIVE
                                    | SCAN_FLAGS_DESCENDING, null, nullRow,
                            bitMap, 0));
            assertEquals(
                    10,
                    scanAllRows("all a", userRowDef.getRowDefId(),
                            SCAN_FLAGS_END_AT_EDGE | SCAN_FLAGS_START_EXCLUSIVE
                                    | SCAN_FLAGS_END_EXCLUSIVE
                                    | SCAN_FLAGS_DESCENDING, nullRow, null,
                            bitMap, 0));

        }

        {
            final RowDef userRowDef = rowDef("aaaa");
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    11110,
                    scanAllRows("all aaaa", rowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE,
                            null, null, bitMap,
                            findIndexId(rowDef, userRowDef, 1)));
            assertEquals(
                    4,
                    scanAllRows("all aaaa", rowDef.getRowDefId(),
                            SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE
                                    | SCAN_FLAGS_SINGLE_ROW, null, null,
                            bitMap, findIndexId(rowDef, userRowDef, 1)));
        }
        {
            final RowDef userRowDef = rowDef("aa");
            int col = findFieldIndex(rowDef, "aa$aa4");
            int indexId = findIndexId(rowDef, "aa$str");
            Object[] startValue = new Object[fc];
            Object[] endValue = new Object[fc];
            startValue[col] = "1";
            endValue[col] = "2";
            start.createRow(rowDef, startValue);
            end.createRow(rowDef, endValue);
            bitMap = bitsToRoot(userRowDef, rowDef);
            assertEquals(
                    13,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), 0, start, end, bitMap,
                            indexId));
            assertEquals(
                    13,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), SCAN_FLAGS_START_EXCLUSIVE,
                            start, end, bitMap, indexId));
            assertEquals(
                    13,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE,
                            start, end, bitMap, indexId));
            assertEquals(
                    2,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), SCAN_FLAGS_END_EXCLUSIVE
                                    | SCAN_FLAGS_SINGLE_ROW, start, end,
                            bitMap, indexId));
            assertEquals(
                    2,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), SCAN_FLAGS_SINGLE_ROW
                                    | SCAN_FLAGS_DESCENDING, start, end,
                            bitMap, indexId));
            assertEquals(
                    2,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(),
                            SCAN_FLAGS_SINGLE_ROW | SCAN_FLAGS_DESCENDING
                                    | SCAN_FLAGS_PREFIX, start, end, bitMap,
                            indexId));
            assertEquals(
                    26,
                    scanAllRows("aa with aa.aa4 in [\"1\", \"2\"]",
                            rowDef.getRowDefId(), SCAN_FLAGS_PREFIX, start,
                            end, bitMap, indexId));
        }
    }

    @Test
    public void testCoveringIndex() throws Exception {
        final RowDef rowDef = rowDef("aaab");
        final int indexId = findIndexId(rowDef, "aaab3aaab1");
        final byte[] columnBitMap = new byte[1];
        columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab1");
        columnBitMap[0] |= 1 << findFieldIndex(rowDef, "aaab3");
        final int count = scanAllRows("Covering index aaab3aaab1",
                rowDef.getRowDefId(), SCAN_FLAGS_START_AT_EDGE
                        | SCAN_FLAGS_END_AT_EDGE, null, null, columnBitMap,
                indexId);
        assertTrue(count > 0);
    }

    @Test
    public void testBug234() throws Exception {
        // Populates the a, aa and aaa tables, but inserts no
        // aaaa rows.
        final RowDef rowDef = rowDef("_akiban_a");
        final RowDef userRowDef = rowDef("aaaa");
        int col = findFieldIndex(rowDef, "aaaa$aaaa1");

        final int fc = rowDef.getFieldCount();
        final RowData start = new RowData(new byte[256]);
        final RowData end = new RowData(new byte[256]);
        byte[] bitMap = bitsToRoot(userRowDef, rowDef);
        Object[] startValue = new Object[fc];
        Object[] endValue = new Object[fc];
        startValue[col] = -3000;
        endValue[col] = -2000;
        start.createRow(rowDef, startValue);
        end.createRow(rowDef, endValue);
        
        bitMap = bitsToRoot(userRowDef, rowDef);

        final RowCollector rc = store.newRowCollector(session, start.getRowDefId(), 0,
                0, start, end, bitMap);
        final ByteBuffer payload = ByteBuffer.allocate(65536);
        // Since there are no aaaa rows, we should get no results
        assertTrue(rc.collectNextRow(payload) == false);
        // Bug 234 was caused by the rc.hasMore() returning true
        // despite there being no aaaa rows left. Therefore
        // this assertion failed.
        assertTrue(rc.hasMore() == false);
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
        final RowDef rowDef = rowDef("aaaa");
        start.createRow(rowDef, new Object[] { null, 123 });
        end.createRow(rowDef, new Object[] { null, 132 });
        final int indexId = findIndexId(rowDef, rowDef, 1);
        byte[] bitMap = new byte[] { 0xF };
        System.out.println("Starting SELECT loop");
        long time = System.nanoTime();
        long iterations = 0;
        for (;;) {
            assertEquals(10,
                    scanAllRows("one aaaa", start, end, bitMap, indexId));
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
