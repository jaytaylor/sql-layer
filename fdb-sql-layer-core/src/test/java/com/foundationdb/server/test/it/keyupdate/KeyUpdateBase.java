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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.value.Value;
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

    protected void confirmColumn(RowType type, Integer expectedId, String columnName) {
        assert columnName != null;
        assertNotNull("column ID for " + columnName, expectedId);
        Column column  = type.fieldColumn(expectedId);
        assertNotNull ("no Column with id = "+expectedId + ", name="+columnName, column);
        assertEquals("Column name", columnName, column.getName());
    }
    
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

    protected void assertSameFields(KeyUpdateRow expected, KeyUpdateRow actual) {
        assertEquals(expected.rowType().table(), actual.rowType().table());
    }

    protected final void dbInsert(final KeyUpdateRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.writeRow(session(), row);
                Integer oldCount = rowDefsToCounts.get(row.rowType().table().getTableId());
                oldCount = (oldCount == null) ? 1 : oldCount+1;
                rowDefsToCounts.put(row.rowType().table().getTableId(), oldCount);
                return null;
            }
        });
    }

    protected final void dbUpdate(final KeyUpdateRow oldRow, final KeyUpdateRow newRow) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.updateRow(session(), oldRow, newRow, null);
                return null;
            }
        });
    }

    protected final void dbDelete(final KeyUpdateRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.deleteRow(session(), row);
                Integer oldCount = rowDefsToCounts.get(row.rowType().table().getTableId());
                assertNotNull(oldCount);
                rowDefsToCounts.put(row.rowType().table().getTableId(), oldCount - 1);
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
                
                assertEquals(testVisitor.records().size(), realVisitor.records().size());
                for (int i = 0; i < testVisitor.records().size(); i++) {
                    assertEquals(testVisitor.records().get(i), realVisitor.records().get(i));
                }
                
                assertEquals(testVisitor.records(), realVisitor.records());
                assertEquals("records count", countAllRows(), testVisitor.records().size());
                // Check indexes
                CollectingIndexKeyVisitor indexVisitor;
                if (checkChildPKs()) {
                    // Vendor PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), vendorRT.table().getPrimaryKey().getIndex(), indexVisitor, -1, 0);
                    assertEquals(vendorPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("vendor PKs", countRows(vendorRT), indexVisitor.records().size());
                    // Customer PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), customerRT.table().getPrimaryKey().getIndex(), indexVisitor, -1, 0);
                    assertEquals(customerPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("customer PKs", countRows(customerRT), indexVisitor.records().size());
                    // Order PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), orderRT.table().getPrimaryKey().getIndex(), indexVisitor, -1, 0);
                    assertEquals(orderPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("order PKs", countRows(orderRT), indexVisitor.records().size());
                    // Item PK index
                    indexVisitor = new CollectingIndexKeyVisitor();
                    testStore.traverse(session(), itemRT.table().getPrimaryKey().getIndex(), indexVisitor, -1, 0);
                    assertEquals(itemPKIndex(testVisitor.records()), indexVisitor.records());
                    assertEquals("order PKs", countRows(itemRT), indexVisitor.records().size());
                }
                // Order priority index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), index(orderRT, "priority"), indexVisitor, -1, 0);
                assertEquals(orderPriorityIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(orderRT), indexVisitor.records().size());
                // Order timestamp index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), index(orderRT, "when"), indexVisitor, -1, 0);
                assertEquals(orderWhenIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(orderRT), indexVisitor.records().size());
                return null;
            }
        });
    }

    private int countRows(RowType rowType) {
        return rowDefsToCounts.get(rowType.table().getTableId());
    }

    private Index index(RowType rowType, String indexName) {
        Index index = rowType.table().getIndex(indexName);
        if (index == null) {
            throw new NoSuchElementException(indexName);
        }
        return index;
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
                expectedCounts.put(vendorRT.table().getTableId(), 0);
                expectedCounts.put(customerRT.table().getTableId(), 0);
                expectedCounts.put(orderRT.table().getTableId(), 0);
                expectedCounts.put(itemRT.table().getTableId(), 0);
                Map<Integer, Integer> actualCounts = new HashMap<>();
                actualCounts.put(customerRT.table().getTableId(), 0);
                actualCounts.put(vendorRT.table().getTableId(), 0);
                actualCounts.put(orderRT.table().getTableId(), 0);
                actualCounts.put(itemRT.table().getTableId(), 0);
                while (expectedIterator.hasNext() && actualIterator.hasNext()) {
                    TreeRecord expected = expectedIterator.next();
                    TreeRecord actual = actualIterator.next();
                    assertEquals(expected, actual);
                    assertEquals(hKey((KeyUpdateRow) expected.row()), actual.hKey());
                    checkInitialState(actual.row());
                    expectedCounts.put(expected.row().rowType().table().getTableId(), 
                            expectedCounts.get(expected.row().rowType().table().getTableId()) + 1);
                    actualCounts.put(actual.row().rowType().table().getTableId(), 
                            actualCounts.get(actual.row().rowType().table().getTableId()) + 1);
                }
                assertEquals(2, expectedCounts.get(vendorRT.table().getTableId()).intValue());
                assertEquals(6, expectedCounts.get(customerRT.table().getTableId()).intValue());
                assertEquals(18, expectedCounts.get(orderRT.table().getTableId()).intValue());
                assertEquals(54, expectedCounts.get(itemRT.table().getTableId()).intValue());
                assertEquals(2, actualCounts.get(vendorRT.table().getTableId()).intValue());
                assertEquals(6, actualCounts.get(customerRT.table().getTableId()).intValue());
                assertEquals(18, actualCounts.get(orderRT.table().getTableId()).intValue());
                assertEquals(54, actualCounts.get(itemRT.table().getTableId()).intValue());
                assertTrue(!expectedIterator.hasNext() && !actualIterator.hasNext());
                return null;
            }
        });
    }

    protected void checkInitialState(Row row)
    {
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            assertEquals(row.value(v_vx).getInt64(), row.value(v_vid).getInt64() * 100);
        } else if (rowType == customerRT) {
            assertEquals(row.value(c_cx).getInt64(), row.value(c_cid).getInt64() * 100);
        } else if (rowType == orderRT) {
            assertEquals(row.value(o_cid).getInt64(), row.value(o_oid).getInt64() / 10);
            assertEquals(row.value(o_ox).getInt64(), row.value(o_oid).getInt64() * 100);
        } else if (rowType == itemRT) {
            assertEquals(row.value(i_oid).getInt64(), row.value(i_iid).getInt64() / 10);
            assertEquals(row.value(i_ix).getInt64(), row.value(i_iid).getInt64() * 100);
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
    protected final List<List<Object>> indexFromRecords(List<TreeRecord> records, RowType rowType, Object... columns) {
        List<List<Object>> indexEntries = new ArrayList<>();
        for (TreeRecord record : records) {
            if (record.row().rowType() == rowType) {
                List<Object> indexEntry = new ArrayList<>(columns.length);
                for (Object column : columns) {
                    final Object indexEntryElement;
                    if (column instanceof Integer) {
                        indexEntryElement = record.row().value( (Integer)column ).getInt64();
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

    protected KeyUpdateRow createTestRow (RowType rowType, Object... values) {
        return new KeyUpdateRow (rowType, store(), values);
    }

    protected KeyUpdateRow updateRow (KeyUpdateRow oldRow, int column, Long newValue, KeyUpdateRow newParent) {
        KeyUpdateRow row = updateRow (oldRow, column, newValue);
        row.parent(newParent);
        KeyUpdateRow newGrandparent = newParent == null ? null : newParent.parent();
        row.hKey(hKey(row, newParent, newGrandparent));
        return row;
    }

    protected KeyUpdateRow updateRow (KeyUpdateRow row, int column, Long newValue) {
        List<Value> values = row.values();
        List<Value> newValues = new ArrayList<Value>(row.rowType().nFields());
        
        for (Value value : values) {
            newValues.add(new Value(value.getType(), value.getInt64()));
        }
        
        if (newValue == null) {
            newValues.get(column).putNull();
        } else {
            newValues.get(column).putInt64(newValue);
        }
        KeyUpdateRow copy = new KeyUpdateRow (row.rowType(), row.getStore(), newValues);
        copy.parent(row.parent());
        copy.hKey(hKey(copy, row.parent()));
        return copy;
    }
       
    protected KeyUpdateRow updateRow (KeyUpdateRow row, int column1, Long newValue1, int column2, Long newValue2) {
        List<Value> values = row.values();
        List<Value> newValues = new ArrayList<Value>(row.rowType().nFields());
        for (Value value : values) {
            newValues.add(new Value(value.getType(), value.getInt64()));
        }
        
        if (newValue1 == null) {
            newValues.get(column1).putNull();
        } else {
            newValues.get(column1).putInt64(newValue1);
        }
        if (newValue2 == null) {
             newValues.get(column2).putNull();
        } else {
            newValues.get(column2).putInt64(newValue2);
        }
        
        KeyUpdateRow copy = new KeyUpdateRow (row.rowType(), row.getStore(), newValues);
        copy.parent(row.parent());
        copy.hKey(hKey(copy, row.parent()));
        return copy;
    }
    
    protected KeyUpdateRow updateRow (KeyUpdateRow row, 
            int column1, Long newValue1, 
            int column2, Long newValue2, 
            KeyUpdateRow newParent) {
        KeyUpdateRow newRow = updateRow(row, column1, newValue1, column2, newValue2);
        newRow.parent(newParent);
        KeyUpdateRow newGrandparent = newParent == null ? null : newParent.parent();
        newRow.hKey(hKey(newRow, newParent, newGrandparent));
        return newRow;
    }

    
    protected KeyUpdateRow copyRow(KeyUpdateRow row)
    {
        KeyUpdateRow copy = new KeyUpdateRow (row.rowType(), row.getStore(), row.values());
        copy.parent(row.parent());
        copy.hKey(hKey(row, row.parent()));
        return copy;
    }

    protected final KeyUpdateRow kurow(RowType type, Object... values) {
        KeyUpdateRow row = new KeyUpdateRow(type, store(), values);
        row.hKey(hKey(row));
        return row;
    }
    

    protected final KeyUpdateRow row (KeyUpdateRow parent, RowType type, Object... values) {
        KeyUpdateRow row = new KeyUpdateRow(type, store(), values);
        row.hKey(hKey(row, parent, null));
        return row;
    }
    
    protected final KeyUpdateRow row (KeyUpdateRow parent, KeyUpdateRow grandparent, RowType type, Object... values) {
        KeyUpdateRow row = new KeyUpdateRow (type, store(), values);
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

    protected HKey hKey(KeyUpdateRow row, KeyUpdateRow newParent)
    {
        return hKey(row, newParent, null);
    }

    private static final String HKEY_PROPAGATION_TAP_PATTERN = ".*propagate_hkey_change.*";

    abstract protected void createSchema() throws Exception;
    abstract protected void populateTables() throws Exception;
    abstract protected boolean checkChildPKs();
    abstract protected HKey hKey(KeyUpdateRow row);
    abstract protected HKey hKey(KeyUpdateRow row, KeyUpdateRow newParent, KeyUpdateRow newGrandparent);
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
