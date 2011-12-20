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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import com.persistit.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static com.akiban.server.test.it.keyupdate.Schema.c_cid;
import static com.akiban.server.test.it.keyupdate.Schema.c_cx;
import static com.akiban.server.test.it.keyupdate.Schema.customerRowDef;
import static com.akiban.server.test.it.keyupdate.Schema.groupRowDef;
import static com.akiban.server.test.it.keyupdate.Schema.i_iid;
import static com.akiban.server.test.it.keyupdate.Schema.i_ix;
import static com.akiban.server.test.it.keyupdate.Schema.i_oid;
import static com.akiban.server.test.it.keyupdate.Schema.itemRowDef;
import static com.akiban.server.test.it.keyupdate.Schema.o_cid;
import static com.akiban.server.test.it.keyupdate.Schema.o_oid;
import static com.akiban.server.test.it.keyupdate.Schema.o_ox;
import static com.akiban.server.test.it.keyupdate.Schema.o_priority;
import static com.akiban.server.test.it.keyupdate.Schema.o_when;
import static com.akiban.server.test.it.keyupdate.Schema.orderRowDef;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

public abstract class KeyUpdateBase extends ITBase {
    @Before
    public final void before() throws Exception
    {
        testStore = new TestStore(store(), persistitStore());
        rowDefsToCounts = new TreeMap<Integer, Integer>();
        createSchema();
        confirmColumns();
        populateTables();
    }

    private void confirmColumns() {
        confirmColumn(customerRowDef, c_cid, "cid");
        confirmColumn(customerRowDef, c_cx, "cx");

        confirmColumn(orderRowDef, o_oid, "oid");
        confirmColumn(orderRowDef, o_cid, "cid");
        confirmColumn(orderRowDef, o_ox, "ox");
        confirmColumn(orderRowDef, o_priority, "priority");
        confirmColumn(orderRowDef, o_when, "when");

        confirmColumn(itemRowDef, i_iid, "iid");
        confirmColumn(itemRowDef, i_oid, "oid");
        confirmColumn(itemRowDef, i_ix, "ix");
    }

    private void confirmColumn(RowDef rowDef, Integer expectedId, String columnName) {
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

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdate() throws Exception
    {
        // Set customer.priority = 80 for order 33
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 80L, null);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderPriorityUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.priority = 81 for order 33. Duplicates are fine.
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_priority, 81L, null);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }


    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdate() throws Exception
    {
        // Set customer.when = 9000 for order 33
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_when, 9000L, null);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderWhenUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.when = 9000 for order 33
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        Long oldWhen = (Long) newOrderRow.put(o_when, 9001L);
        assertEquals("old order.when", Long.valueOf(9009L), oldWhen);
        try {
            dbUpdate(oldOrderRow, newOrderRow);

            // Make sure such a row actually exists!
            TestRow shouldHaveConflicted = testStore.find(new HKey(customerRowDef, 1L, orderRowDef, 11L));
            assertNotNull("shouldHaveConflicted not found", shouldHaveConflicted);
            assertEquals(9001L, shouldHaveConflicted.getFields().get(o_when));

            fail("update should have failed with duplicate key");
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        TestRow confirmOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        assertSameFields(oldOrderRow, confirmOrderRow);
        checkDB();
    }

    @Test
    @SuppressWarnings("unused") // JUnit will invoke this
    public void testOrderUpdateIsNoOp() throws Exception
    {
        // Update a row to its same values
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 3L, orderRowDef, 33L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    private void assertSameFields(TestRow expected, TestRow actual) {
        Map<Integer,Object> expectedFields = expected.getFields();
        Map<Integer,Object> actualFields = actual.getFields();
        if (!expectedFields.equals(actualFields)) {
            TreeMap<Integer,Object> expectedSorted = new TreeMap<Integer, Object>(expectedFields);
            TreeMap<Integer,Object> actualSorted = new TreeMap<Integer, Object>(actualFields);
            assertEquals(expectedSorted, actualSorted);
            fail("if they're not equal, we shouldn't have gotten here!");
        }
    }

    protected final void dbInsert(TestRow row) throws Exception
    {
        Transaction txn = treeService().getTransaction(session());
        txn.begin();
        try {
            testStore.writeRow(session(), row);
            Integer oldCount = rowDefsToCounts.get(row.getTableId());
            oldCount = (oldCount == null) ? 1 : oldCount+1;
            rowDefsToCounts.put(row.getTableId(), oldCount);
            txn.commit();
        }
        finally {
            txn.end();
        }
    }

    protected final void dbUpdate(TestRow oldRow, TestRow newRow) throws Exception
    {
        Transaction txn = treeService().getTransaction(session());
        txn.begin();
        try {
            testStore.updateRow(session(), oldRow, newRow, null);
            txn.commit();
        }
        finally {
            txn.end();
        }
    }

    protected final void dbDelete(TestRow row) throws Exception
    {
        Transaction txn = treeService().getTransaction(session());
        txn.begin();
        try {
            testStore.deleteRow(session(), row);
            Integer oldCount = rowDefsToCounts.get(row.getTableId());
            assertNotNull(oldCount);
            rowDefsToCounts.put(row.getTableId(), oldCount - 1);
            txn.commit();
        }
        finally {
            txn.end();
        }
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
        Transaction trx = treeService().getTransaction(session());
        trx.begin();
        try {
            // Records
            RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
            RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
            testStore.traverse(session(), groupRowDef, testVisitor, realVisitor);
            assertEquals(testVisitor.records(), realVisitor.records());
            assertEquals("records count", countAllRows(), testVisitor.records().size());
            // Check indexes
            CollectingIndexKeyVisitor indexVisitor;
            if (checkChildPKs()) {
                // Customer PK index - skip. This index is hkey equivalent, and we've already checked the full records.
                // Order PK index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), orderRowDef.getPKIndex(), indexVisitor);
                assertEquals(orderPKIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(orderRowDef), indexVisitor.records().size());
                // Item PK index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), itemRowDef.getPKIndex(), indexVisitor);
                assertEquals(itemPKIndex(testVisitor.records()), indexVisitor.records());
                assertEquals("order PKs", countRows(itemRowDef), indexVisitor.records().size());
            }
            // Order priority index
            indexVisitor = new CollectingIndexKeyVisitor();
            testStore.traverse(session(), index(orderRowDef, "priority"), indexVisitor);
            assertEquals(orderPriorityIndex(testVisitor.records()), indexVisitor.records());
            assertEquals("order PKs", countRows(orderRowDef), indexVisitor.records().size());
            // Order timestamp index
            indexVisitor = new CollectingIndexKeyVisitor();
            testStore.traverse(session(), index(orderRowDef, "when"), indexVisitor);
            assertEquals(orderWhenIndex(testVisitor.records()), indexVisitor.records());
            assertEquals("order PKs", countRows(orderRowDef), indexVisitor.records().size());

            trx.commit();
        }
        finally {
            trx.end();
        }
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
        Transaction trx = treeService().getTransaction(session());
        trx.begin();
        try {
            RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
            RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
            testStore.traverse(session(), groupRowDef, testVisitor, realVisitor);
            Iterator<TreeRecord> expectedIterator = testVisitor.records().iterator();
            Iterator<TreeRecord> actualIterator = realVisitor.records().iterator();
            Map<Integer, Integer> expectedCounts = new HashMap<Integer, Integer>();
            expectedCounts.put(customerRowDef.getRowDefId(), 0);
            expectedCounts.put(orderRowDef.getRowDefId(), 0);
            expectedCounts.put(itemRowDef.getRowDefId(), 0);
            Map<Integer, Integer> actualCounts = new HashMap<Integer, Integer>();
            actualCounts.put(customerRowDef.getRowDefId(), 0);
            actualCounts.put(orderRowDef.getRowDefId(), 0);
            actualCounts.put(itemRowDef.getRowDefId(), 0);
            while (expectedIterator.hasNext() && actualIterator.hasNext()) {
                TreeRecord expected = expectedIterator.next();
                TreeRecord actual = actualIterator.next();
                assertEquals(expected, actual);
                assertEquals(hKey((TestRow) expected.row()), actual.hKey());
                checkInitialState(actual.row());
                expectedCounts.put(expected.row().getTableId(), expectedCounts.get(expected.row().getTableId()) + 1);
                actualCounts.put(actual.row().getTableId(), actualCounts.get(actual.row().getTableId()) + 1);
            }
            assertEquals(3, expectedCounts.get(customerRowDef.getRowDefId()).intValue());
            assertEquals(9, expectedCounts.get(orderRowDef.getRowDefId()).intValue());
            assertEquals(27, expectedCounts.get(itemRowDef.getRowDefId()).intValue());
            assertEquals(3, actualCounts.get(customerRowDef.getRowDefId()).intValue());
            assertEquals(9, actualCounts.get(orderRowDef.getRowDefId()).intValue());
            assertEquals(27, actualCounts.get(itemRowDef.getRowDefId()).intValue());
            assertTrue(!expectedIterator.hasNext() && !actualIterator.hasNext());

            trx.commit();
        }
        finally {
            trx.end();
        }
    }

    protected final void checkInitialState(NewRow row)
    {
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            assertEquals(row.get(c_cx), ((Long)row.get(c_cid)) * 100);
        } else if (rowDef == orderRowDef) {
            assertEquals(row.get(o_cid), ((Long)row.get(o_oid)) / 10);
            assertEquals(row.get(o_ox), ((Long)row.get(o_oid)) * 100);
        } else if (rowDef == itemRowDef) {
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
        List<List<Object>> indexEntries = new ArrayList<List<Object>>();
        for (TreeRecord record : records) {
            if (record.row().getRowDef() == rowDef) {
                List<Object> indexEntry = new ArrayList<Object>(columns.length);
                for (Object column : columns) {
                    final Object indexEntryElement;
                    if (column instanceof Integer) {
                        indexEntryElement = record.row().get( (Integer)column );
                    }
                    else if (column instanceof HKeyElement) {
                        indexEntryElement = record.hKey().objectArray()[ ((HKeyElement) column).getIndex() ];
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

    protected TestRow copyRow(TestRow row)
    {
        TestRow copy = new TestRow(row.getTableId(), store());
        for (Map.Entry<Integer, Object> entry : row.getFields().entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        copy.parent(row.parent());
        copy.hKey(hKey(row, row.parent()));
        return copy;
    }

    protected void updateRow(TestRow row, int column, Object newValue, TestRow newParent)
    {
        row.put(column, newValue);
        row.parent(newParent);
        row.hKey(hKey(row, newParent));
    }

    protected final TestRow row(RowDef table, Object... values)
    {
        TestRow row = new TestRow(table.getRowDefId(), store());
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

    abstract protected void createSchema() throws Exception;
    abstract protected void populateTables() throws Exception;
    abstract protected boolean checkChildPKs();
    abstract protected HKey hKey(TestRow row);
    abstract protected HKey hKey(TestRow row, TestRow newParent);
    abstract protected List<List<Object>> orderPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> itemPKIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records);
    abstract protected List<List<Object>> orderWhenIndex(List<TreeRecord> records);

    protected TestStore testStore;
    protected Map<Integer,Integer> rowDefsToCounts;
    
}
