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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Index;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.*;

public abstract class KeyUpdateBase extends ITBase {
    @Before
    public final void before() throws Exception
    {
        testStore = new TestStore(store());
        rowDefsToCounts = new TreeMap<>();
        createSchema();
        confirmColumns();
        populateTables();
    }

    protected abstract void confirmColumns();

    protected void confirmColumn(RowDef rowDef, Integer expectedId, String columnName) {
        assert columnName != null;
        assert rowDef != null;
        assertNotNull("column ID for " + columnName, expectedId);
        FieldDef fieldDef = rowDef.getFieldDef(expectedId);
        assertNotNull("no fieldDef with id="+expectedId + ", name="+columnName, fieldDef);
        assertEquals("fieldDef name", columnName, fieldDef.getName());
    }


    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testInitialState() throws Exception
    {
        checkDB();
        checkInitialState();
    }

    protected void assertSameFields(TestRow expected, TestRow actual) {
        Map<Integer,Object> expectedFields = expected.getFields();
        Map<Integer,Object> actualFields = actual.getFields();
        if (!expectedFields.equals(actualFields)) {
            TreeMap<Integer,Object> expectedSorted = new TreeMap<>(expectedFields);
            TreeMap<Integer,Object> actualSorted = new TreeMap<>(actualFields);
            assertEquals(expectedSorted, actualSorted);
            fail("if they're not equal, we shouldn't have gotten here!");
        }
    }

    protected final void dbInsert(final TestRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.writeRow(session(), row);
                Integer oldCount = rowDefsToCounts.get(row.getTableId());
                oldCount = (oldCount == null) ? 1 : oldCount+1;
                rowDefsToCounts.put(row.getTableId(), oldCount);
                return null;
            }
        });
    }

    protected final void dbUpdate(final TestRow oldRow, final TestRow newRow) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.updateRow(session(), oldRow, newRow, null);
                return null;
            }
        });
    }

    protected final void dbDelete(final TestRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.deleteRow(session(), row);
                Integer oldCount = rowDefsToCounts.get(row.getTableId());
                assertNotNull(oldCount);
                rowDefsToCounts.put(row.getTableId(), oldCount - 1);
                return null;
            }
        });
    }

    private int countAllRows() {
        int total = 0;
        for (Integer count : rowDefsToCounts.values()) {
            total += count;
        }
        return total;
    }

    protected final void checkDB() throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                // Records
                RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
                RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
                testStore.traverse(session(), group, testVisitor, realVisitor);
                assertEquals(testVisitor.records(), realVisitor.records());
                assertEquals("records count", countAllRows(), testVisitor.records().size());
                // Check indexes
                CollectingIndexKeyVisitor indexVisitor;
                if (checkChildPKs()) {
                    // Vendor PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), vendorRD.getPKIndex(), indexVisitor, -1, 0);
                    assertEquals(vendorPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("vendor PKs", countRows(vendorRD), indexVisitor.records().size());
                    // Customer PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), customerRD.getPKIndex(), indexVisitor, -1, 0);
                    assertEquals(customerPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("customer PKs", countRows(customerRD), indexVisitor.records().size());
                    // Order PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), orderRD.getPKIndex(), indexVisitor, -1, 0);
                    assertEquals(orderPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("order PKs", countRows(orderRD), indexVisitor.records().size());
                    // Item PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), itemRD.getPKIndex(), indexVisitor, -1, 0);
                    assertEquals(itemPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("order PKs", countRows(itemRD), indexVisitor.records().size());
                }
                // Order priority index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), index(orderRD, "priority"), indexVisitor, -1, 0);
                assertEquals(orderPriorityIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(orderRD), indexVisitor.records().size());
                // Order timestamp index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), index(orderRD, "when"), indexVisitor, -1, 0);
                assertEquals(orderWhenIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(orderRD), indexVisitor.records().size());
                return null;
            }
        });
    }

    private int countRows(RowDef rowDef) {
        return rowDefsToCounts.get(rowDef.getRowDefId());
    }

    private Index index(RowDef rowDef, String indexName) {
        for (Index index : rowDef.getIndexes()) {
            if (indexName.equals(index.getIndexName().getName())) {
                return index;
            }
        }
        throw new NoSuchElementException(indexName);
    }

    protected final void checkInitialState() throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
                RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
                testStore.traverse(session(), group, testVisitor, realVisitor);
                Iterator<TreeRecord> expectedIterator = testVisitor.records().iterator();
                Iterator<TreeRecord> actualIterator = realVisitor.records().iterator();
                Map<Integer, Integer> expectedCounts = new HashMap<>();
                expectedCounts.put(vendorRD.getRowDefId(), 0);
                expectedCounts.put(customerRD.getRowDefId(), 0);
                expectedCounts.put(orderRD.getRowDefId(), 0);
                expectedCounts.put(itemRD.getRowDefId(), 0);
                Map<Integer, Integer> actualCounts = new HashMap<>();
                actualCounts.put(customerRD.getRowDefId(), 0);
                actualCounts.put(vendorRD.getRowDefId(), 0);
                actualCounts.put(orderRD.getRowDefId(), 0);
                actualCounts.put(itemRD.getRowDefId(), 0);
                while (expectedIterator.hasNext() && actualIterator.hasNext()) {
                    TreeRecord expected = expectedIterator.next();
                    TreeRecord actual = actualIterator.next();
                    assertEquals(expected, actual);
                    assertEquals(hKey((TestRow) expected.row()), actual.hKey());
                    checkInitialState(actual.row());
                    expectedCounts.put(expected.row().getTableId(), expectedCounts.get(expected.row().getTableId()) + 1);
                    actualCounts.put(actual.row().getTableId(), actualCounts.get(actual.row().getTableId()) + 1);
                }
                assertEquals(2, expectedCounts.get(vendorRD.getRowDefId()).intValue());
                assertEquals(6, expectedCounts.get(customerRD.getRowDefId()).intValue());
                assertEquals(18, expectedCounts.get(orderRD.getRowDefId()).intValue());
                assertEquals(54, expectedCounts.get(itemRD.getRowDefId()).intValue());
                assertEquals(2, actualCounts.get(vendorRD.getRowDefId()).intValue());
                assertEquals(6, actualCounts.get(customerRD.getRowDefId()).intValue());
                assertEquals(18, actualCounts.get(orderRD.getRowDefId()).intValue());
                assertEquals(54, actualCounts.get(itemRD.getRowDefId()).intValue());
                assertTrue(!expectedIterator.hasNext() && !actualIterator.hasNext());
                return null;
            }
        });
    }

    protected void checkInitialState(NewRow row)
    {
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            assertEquals(row.get(v_vx), ((Long)row.get(v_vid)) * 100);
        } else if (rowDef == customerRD) {
            assertEquals(row.get(c_cx), ((Long)row.get(c_cid)) * 100);
        } else if (rowDef == orderRD) {
            assertEquals(row.get(o_cid), ((Long)row.get(o_oid)) / 10);
            assertEquals(row.get(o_ox), ((Long)row.get(o_oid)) * 100);
        } else if (rowDef == itemRD) {
            assertEquals(row.get(i_oid), ((Long)row.get(i_iid)) / 10);
            assertEquals(row.get(i_ix), ((Long)row.get(i_iid)) * 100);
        } else {
            fail();
        }
    }

    /**
     * Given a list of records, a RowDef and a list of columns, extracts the index entries.
     * @param records the records to take entries from
     * @param rowDef the rowdef of records to look at
     * @param columns a union of either Integer (the column ID) or HKeyElement.
     * Any other types will throw a RuntimeException
     * @return a list representing indexes of these records
     */
    protected final List<List<Object>> indexFromRecords(List<TreeRecord> records, RowDef rowDef, Object... columns) {
        List<List<Object>> indexEntries = new ArrayList<>();
        for (TreeRecord record : records) {
            if (record.row().getRowDef() == rowDef) {
                List<Object> indexEntry = new ArrayList<>(columns.length);
                for (Object column : columns) {
                    final Object indexEntryElement;
                    if (column instanceof Integer) {
                        indexEntryElement = record.row().get( (Integer)column );
                    }
                    else if (column instanceof HKeyElement) {
                        indexEntryElement = record.hKey().objectArray()[ ((HKeyElement) column).getIndex() ];
                    }
                    else if (column instanceof NullSeparatorColumn) {
                        indexEntryElement = 0L;
                    }
                    else {
                        String msg = String.format(
                                "column must be an Integer or HKeyElement: %s in %s:",
                                column == null ? "null" : column.getClass().getName(),
                                Arrays.toString(columns)
                        );
                        throw new RuntimeException(msg);
                    }
                    indexEntry.add(indexEntryElement);
                }
                indexEntries.add(indexEntry);
            }
        }
        Collections.sort(indexEntries,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> x, List<Object> y) {
                        // compare priorities
                        Long px = (Long) x.get(0);
                        Long py = (Long) y.get(0);
                        return px.compareTo(py);
                    }
                }
        );
        return indexEntries;
    }

    protected TestRow createTestRow(int tableId) {
        return new TestRow(tableId, getRowDef(tableId), store());
    }

    protected TestRow createTestRow(RowDef rowDef) {
        return new TestRow(rowDef.getRowDefId(), rowDef, store());
    }

    protected TestRow copyRow(TestRow row)
    {
        TestRow copy = createTestRow(row.getTableId());
        for (Map.Entry<Integer, Object> entry : row.getFields().entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        copy.parent(row.parent());
        copy.hKey(hKey(row, row.parent()));
        return copy;
    }

    protected void updateRow(TestRow row, int column, Object newValue)
    {
        row.put(column, newValue);
        row.hKey(hKey(row));
    }

    protected void updateRow(TestRow row, int column, Object newValue, TestRow newParent)
    {
        row.put(column, newValue);
        row.parent(newParent);
        TestRow newGrandparent = newParent == null ? null : newParent.parent();
        row.hKey(hKey(row, newParent, newGrandparent));
    }

    protected final TestRow row(RowDef table, Object... values)
    {
        TestRow row = createTestRow(table);
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row));
        return row;
    }

    protected TestRow row(TestRow parent, RowDef table, Object... values)
    {
        TestRow row = createTestRow(table);
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
               value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row, parent, null));
        return row;
    }

    protected TestRow row(TestRow parent, TestRow grandparent, RowDef table, Object... values)
    {
        TestRow row = createTestRow(table);
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row, parent, grandparent));
        return row;
    }

    protected static final class HKeyElement {
        private final int index;

        public static HKeyElement from(int index) {
            return new HKeyElement(index);
        }

        public HKeyElement(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    protected void startMonitoringHKeyPropagation()
    {
        Tap.setEnabled(HKEY_PROPAGATION_TAP_PATTERN, true);
        Tap.reset(HKEY_PROPAGATION_TAP_PATTERN);
    }

    protected void checkHKeyPropagation(int propagateDownGroupCalls, int propagateDownGroupRowReplace)
    {
        for (TapReport report : Tap.getReport(HKEY_PROPAGATION_TAP_PATTERN)) {
            if (report.getName().endsWith("propagate_hkey_change")) {
                assertEquals(propagateDownGroupCalls, report.getInCount());
            } else if (report.getName().endsWith("propagate_hkey_change_row_replace")) {
                assertEquals(propagateDownGroupRowReplace, report.getInCount());
            } else {
                fail();
            }
        }
    }

    protected HKey hKey(TestRow row, TestRow newParent)
    {
        return hKey(row, newParent, null);
    }

    private static final String HKEY_PROPAGATION_TAP_PATTERN = ".*propagate_hkey_change.*";

    abstract protected void createSchema() throws Exception;
    abstract protected void populateTables() throws Exception;
    abstract protected boolean checkChildPKs();
    abstract protected HKey hKey(TestRow row);
    abstract protected HKey hKey(TestRow row, TestRow newParent, TestRow newGrandparent);
    abstract protected List<List<Object>> vendorPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> customerPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> orderPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> itemPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> orderWhenIndex(List<TreeRecord> records);

    protected TestStore testStore;
    protected Map<Integer,Integer> rowDefsToCounts;

    protected static class NullSeparatorColumn {}
}
