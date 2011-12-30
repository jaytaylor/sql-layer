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

import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import com.persistit.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;

import static com.akiban.server.test.it.keyupdate.Schema.*;
import static junit.framework.Assert.*;

public class KeyUpdateWithMoreComplexSchemaIT extends ITBase
{
    @Before
    public void before() throws Exception
    {
        testStore = new TestStore(store(), persistitStore());
        createSchema();
        populateTables();
    }

    @Test
    public void testInitialState() throws Exception
    {
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid1 = 0, item.oid2 = 0 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, 222L, 222000L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, i_oid1, 0L, null);
        updateRow(newRow, i_oid2, 0L, null);
        dbUpdate(oldRow, newRow);
        checkDB();
        // Revert change
        dbUpdate(newRow, oldRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdate() throws Exception
    {
        // Set item.iid = 0 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, 222L, 222000L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        assertNotNull(parent);
        updateRow(newRow, i_iid1, 0L, parent);
        updateRow(newRow, i_iid2, 0L, parent);
        dbUpdate(oldRow, newRow);
        checkDB();
        // Revert change
        dbUpdate(newRow, oldRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdateCreatingDuplicate() throws Exception
    {
        // Set item.iid = 223 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, 222L, 222000L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        assertNotNull(parent);
        updateRow(newRow, i_iid1, 223L, parent);
        updateRow(newRow, i_iid2, 223000L, parent);
        try {
            dbUpdate(oldRow, newRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderFKUpdate() throws Exception
    {
        // Set order.cid = 0 for order 22
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_cid1, 0L, null);
        updateRow(newOrderRow, o_cid2, 0L, null);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, newOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 0L, 0L, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, oldOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(newOrderRow, oldOrderRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 22
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_oid1, 0L, null);
        updateRow(newOrderRow, o_oid2, 0L, null);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, null, null, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, oldOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(newOrderRow, oldOrderRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 21 for order 22
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L, 2000L));
        assertNotNull(parent);
        updateRow(newRow, o_oid1, 21L, parent);
        updateRow(newRow, o_oid2, 21000L, parent);
        try {
            dbUpdate(oldRow, newRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 2
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 2L, 2000L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid1, 0L, null);
        updateRow(newCustomerRow, c_cid2, 0L, null);
        dbUpdate(oldCustomerRow, newCustomerRow);
        checkDB();
        // Revert change
        dbUpdate(newCustomerRow, oldCustomerRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 1 for customer 3
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 3L, 3000L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid1, 1L, null);
        updateRow(newCustomerRow, c_cid2, 1000L, null);
        try {
            dbUpdate(oldCustomerRow, newCustomerRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testItemDelete() throws Exception
    {
        TestRow itemRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, 222L, 222000L));
        dbDelete(itemRow);
        checkDB();
        // Revert change
        dbInsert(itemRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderDelete() throws Exception
    {
        TestRow orderRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L));
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, 2000L, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbDelete(orderRow);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, null, null, orderRowDef, 22L, 22000L, itemRowDef, iid, iid * 1000));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, orderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbInsert(orderRow);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerDelete() throws Exception
    {
        TestRow customerRow = testStore.find(new HKey(customerRowDef, 2L, 2000L));
        dbDelete(customerRow);
        checkDB();
        // Revert change
        dbInsert(customerRow);
        checkDB();
        checkInitialState();
    }

    private void createSchema() throws InvalidOperationException
    {
        // customer
        customerId = createTable("coi", "customer",
                                 "s1 varchar(10)",
                                 "s2 varchar(10)",
                                 "cid1 int not null",
                                 "s3 varchar(10)",
                                 "s4 varchar(10)",
                                 "cid2 int not null",
                                 "s5 varchar(10)",
                                 "s6 varchar(10)",
                                 "primary key(cid1, cid2)");
        c_s1 = 0;
        c_s2 = 1;
        c_cid1 = 2;
        c_s3 = 3;
        c_s4 = 4;
        c_cid2 = 5;
        c_s5 = 6;
        c_s6 = 7;
        // order
        orderId = createTable("coi", "order",
                              "s1 varchar(10)",
                              "oid1 int not null",
                              "s2 varchar(10)",
                              "s3 varchar(10)",
                              "cid1 int",
                              "s4 varchar(10)",
                              "s5 varchar(10)",
                              "cid2 int",
                              "s6 varchar(10)",
                              "s7 varchar(10)",
                              "oid2 int not null",
                              "s8 varchar(10)",
                              "primary key(oid1, oid2)",
                              "constraint __akiban_oc foreign key __akiban_oc(cid1, cid2) references customer(cid1, cid2)");
        o_s1 = 0;
        o_oid1 = 1;
        o_s2 = 2;
        o_s3 = 3;
        o_cid1 = 4;
        o_s4 = 5;
        o_s5 = 6;
        o_cid2 = 7;
        o_s6 = 8;
        o_s7 = 9;
        o_oid2 = 10;
        o_s8 = 11;
        // item
        itemId = createTable("coi", "item",
                             "s1 varchar(10)",
                             "iid2 int not null",
                             "s2 varchar(10)",
                             "iid1 int not null",
                             "s3 varchar(10)",
                             "oid1 int",
                             "s4 varchar(10)",
                             "oid2 int",
                             "s5 varchar(10)",
                             "primary key(iid1, iid2, oid1, oid2)",
                             "constraint __akiban_io foreign key __akiban_io(oid1, oid2) references order(oid1, oid2)");
        i_s1 = 0;
        i_iid2 = 1;
        i_s2 = 2;
        i_iid1 = 3;
        i_s3 = 4;
        i_oid1 = 5;
        i_s4 = 6;
        i_oid2 = 7;
        i_s5 = 8;
        orderRowDef = rowDefCache().getRowDef(orderId);
        customerRowDef = rowDefCache().getRowDef(customerId);
        itemRowDef = rowDefCache().getRowDef(itemId);
        // group
        int groupRowDefId = customerRowDef.getGroupRowDefId();
        groupRowDef = store().getRowDefCache().getRowDef(groupRowDefId);
    }

    private void updateRow(TestRow row, int column, Object newValue, TestRow newParent)
    {
        row.put(column, newValue);
        row.parent(newParent);
        row.hKey(hKey(row, newParent));
    }

    private void checkDB()
        throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                // Records
                RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
                RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
                testStore.traverse(session(), groupRowDef, testVisitor, realVisitor);
                assertEquals(testVisitor.records(), realVisitor.records());
                // Check indexes
                CollectingIndexKeyVisitor indexVisitor;
                // Customer PK index - skip. This index is hkey equivalent, and we've already checked the full records.
                // Order PK index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), orderRowDef.getPKIndex(), indexVisitor);
                assertEquals(orderPKIndex(testVisitor.records()), indexVisitor.records());
                // Item PK index
                indexVisitor = new CollectingIndexKeyVisitor();
                testStore.traverse(session(), itemRowDef.getPKIndex(), indexVisitor);
                assertEquals(itemPKIndex(testVisitor.records()), indexVisitor.records());
                return null;
            }
        });
    }

    private void checkInitialState() throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
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
                return null;
            }
        });
    }

    private void checkInitialState(NewRow row)
    {
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            assertEquals(row.get(c_cid2), ((Long)row.get(c_cid1)) * 1000);
            assertEquals(F, row.get(c_s1));
            assertEquals(F, row.get(c_s2));
            assertEquals(F, row.get(c_s3));
            assertEquals(F, row.get(c_s4));
            assertEquals(F, row.get(c_s5));
            assertEquals(F, row.get(c_s6));
        } else if (rowDef == orderRowDef) {
            assertEquals(row.get(o_oid2), (Long) row.get(o_oid1) * 1000);
            assertEquals(row.get(o_cid1), ((Long)row.get(o_oid1)) / 10);
            assertEquals(row.get(o_cid2), ((Long)row.get(o_cid1)) * 1000);
            assertEquals(F, row.get(o_s1));
            assertEquals(F, row.get(o_s2));
            assertEquals(F, row.get(o_s3));
            assertEquals(F, row.get(o_s4));
            assertEquals(F, row.get(o_s5));
            assertEquals(F, row.get(o_s6));
            assertEquals(F, row.get(o_s7));
            assertEquals(F, row.get(o_s8));
        } else if (rowDef == itemRowDef) {
            assertEquals(row.get(i_iid2), (Long) row.get(i_iid1) * 1000);
            assertEquals(row.get(i_oid1), ((Long)row.get(i_iid1)) / 10);
            assertEquals(row.get(i_oid2), ((Long)row.get(i_oid1)) * 1000);
            assertEquals(F, row.get(i_s1));
            assertEquals(F, row.get(i_s2));
            assertEquals(F, row.get(i_s3));
            assertEquals(F, row.get(i_s4));
            assertEquals(F, row.get(i_s5));
        } else {
            fail();
        }
    }

    private List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        List<List<Object>> indexEntries = new ArrayList<List<Object>>();
        for (TreeRecord record : records) {
            if (record.row().getRowDef() == orderRowDef) {
                List<Object> indexEntry =
                    Arrays.asList(record.row().get(o_oid1),
                                  record.row().get(o_oid2),
                                  record.row().get(o_cid1),
                                  record.row().get(o_cid2));
                indexEntries.add(indexEntry);
            }
        }
        Collections.sort(indexEntries,
                         new Comparator<List<Object>>()
                         {
                             @Override
                             public int compare(List<Object> x, List<Object> y)
                             {
                                 Long lx = (Long) x.get(0);
                                 Long ly = (Long) y.get(0);
                                 return lx < ly ? -1 : lx > ly ? 1 : 0;
                             }
                         });
        return indexEntries;
    }

    private List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        List<List<Object>> indexEntries = new ArrayList<List<Object>>();
        for (TreeRecord record : records) {
            if (record.row().getRowDef() == itemRowDef) {
                List<Object> indexEntry =
                    Arrays.asList(record.row().get(i_iid1), // iid1
                                  record.row().get(i_iid2), // iid2
                                  record.row().get(i_oid1), // oid1
                                  record.row().get(i_oid2), // oid2
                                  record.hKey().objectArray()[1], // cid1
                                  record.hKey().objectArray()[2]); // cid2
                indexEntries.add(indexEntry);
            }
        }
        Collections.sort(indexEntries,
                         new Comparator<List<Object>>()
                         {
                             @Override
                             public int compare(List<Object> x, List<Object> y)
                             {
                                 Long lx = (Long) x.get(0);
                                 Long ly = (Long) y.get(0);
                                 return lx < ly ? -1 : lx > ly ? 1 : 0;
                             }
                         });
        return indexEntries;
    }

    private void populateTables() throws Exception
    {
        TestRow order;
        // xid2 = xid1 * 1000
        dbInsert(row(customerRowDef, F, F, 1, F, F, 1000, F, F));
        dbInsert(order = row(orderRowDef, F, 11, F, F, 1, F, F, 1000, F, F, 11000, F));
        dbInsert(row(order, itemRowDef, F, 111000, F, 111, F, 11, F, 11000, F));
        dbInsert(row(order, itemRowDef, F, 112000, F, 112, F, 11, F, 11000, F));
        dbInsert(row(order, itemRowDef, F, 113000, F, 113, F, 11, F, 11000, F));
        dbInsert(order = row(orderRowDef, F, 12, F, F, 1, F, F, 1000, F, F, 12000, F));
        dbInsert(row(order, itemRowDef, F, 121000, F, 121, F, 12, F, 12000, F));
        dbInsert(row(order, itemRowDef, F, 122000, F, 122, F, 12, F, 12000, F));
        dbInsert(row(order, itemRowDef, F, 123000, F, 123, F, 12, F, 12000, F));
        dbInsert(order = row(orderRowDef, F, 13, F, F, 1, F, F, 1000, F, F, 13000, F));
        dbInsert(row(order, itemRowDef, F, 131000, F, 131, F, 13, F, 13000, F));
        dbInsert(row(order, itemRowDef, F, 132000, F, 132, F, 13, F, 13000, F));
        dbInsert(row(order, itemRowDef, F, 133000, F, 133, F, 13, F, 13000, F));
        dbInsert(row(customerRowDef, F, F, 2, F, F, 2000, F, F));
        dbInsert(order = row(orderRowDef, F, 21, F, F, 2, F, F, 2000, F, F, 21000, F));
        dbInsert(row(order, itemRowDef, F, 211000, F, 211, F, 21, F, 21000, F));
        dbInsert(row(order, itemRowDef, F, 212000, F, 212, F, 21, F, 21000, F));
        dbInsert(row(order, itemRowDef, F, 213000, F, 213, F, 21, F, 21000, F));
        dbInsert(order = row(orderRowDef, F, 22, F, F, 2, F, F, 2000, F, F, 22000, F));
        dbInsert(row(order, itemRowDef, F, 221000, F, 221, F, 22, F, 22000, F));
        dbInsert(row(order, itemRowDef, F, 222000, F, 222, F, 22, F, 22000, F));
        dbInsert(row(order, itemRowDef, F, 223000, F, 223, F, 22, F, 22000, F));
        dbInsert(order = row(orderRowDef, F, 23, F, F, 2, F, F, 2000, F, F, 23000, F));
        dbInsert(row(order, itemRowDef, F, 231000, F, 231, F, 23, F, 23000, F));
        dbInsert(row(order, itemRowDef, F, 232000, F, 232, F, 23, F, 23000, F));
        dbInsert(row(order, itemRowDef, F, 233000, F, 233, F, 23, F, 23000, F));
        dbInsert(row(customerRowDef, F, F, 3, F, F, 3000, F, F));
        dbInsert(order = row(orderRowDef, F, 31, F, F, 3, F, F, 3000, F, F, 31000, F));
        dbInsert(row(order, itemRowDef, F, 311000, F, 311, F, 31, F, 31000, F));
        dbInsert(row(order, itemRowDef, F, 312000, F, 312, F, 31, F, 31000, F));
        dbInsert(row(order, itemRowDef, F, 313000, F, 313, F, 31, F, 31000, F));
        dbInsert(order = row(orderRowDef, F, 32, F, F, 3, F, F, 3000, F, F, 32000, F));
        dbInsert(row(order, itemRowDef, F, 321000, F, 321, F, 32, F, 32000, F));
        dbInsert(row(order, itemRowDef, F, 322000, F, 322, F, 32, F, 32000, F));
        dbInsert(row(order, itemRowDef, F, 323000, F, 323, F, 32, F, 32000, F));
        dbInsert(order = row(orderRowDef, F, 33, F, F, 3, F, F, 3000, F, F, 33000, F));
        dbInsert(row(order, itemRowDef, F, 331000, F, 331, F, 33, F, 33000, F));
        dbInsert(row(order, itemRowDef, F, 332000, F, 332, F, 33, F, 33000, F));
        dbInsert(row(order, itemRowDef, F, 333000, F, 333, F, 33, F, 33000, F));
    }

    private TestRow row(RowDef table, Object... values)
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

    private TestRow row(TestRow parent, RowDef table, Object... values)
    {
        TestRow row = new TestRow(table.getRowDefId(), store());
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row, parent));
        return row;
    }

    private void dbInsert(final TestRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.writeRow(session(), row);
                return null;
            }
        });
    }

    private void dbUpdate(final TestRow oldRow, final TestRow newRow) throws Exception
    {
        transactionally(new Callable<Void>() {
           public Void call() throws Exception {
                testStore.updateRow(session(), oldRow, newRow, null);
                return null;
           }
        });
    }

    private void dbDelete(final TestRow row) throws Exception
    {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                testStore.deleteRow(session(), row);
                return null;
            }
        });
    }

    private HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            hKey = new HKey(customerRowDef, row.get(c_cid1), row.get(c_cid2));
        } else if (rowDef == orderRowDef) {
            hKey = new HKey(customerRowDef, row.get(o_cid1), row.get(o_cid2),
                            orderRowDef, row.get(o_oid1), row.get(o_oid2));
        } else if (rowDef == itemRowDef) {
            assertNotNull(row.parent());
            hKey = new HKey(customerRowDef, row.parent().get(o_cid1), row.parent().get(o_cid2),
                            orderRowDef, row.get(i_oid1), row.get(i_oid2),
                            itemRowDef, row.get(i_iid1), row.get(i_iid2));
        } else {
            fail();
        }
        return hKey;
    }

    private HKey hKey(TestRow row, TestRow parent)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            hKey = new HKey(customerRowDef, row.get(c_cid1), row.get(c_cid2));
        } else if (rowDef == orderRowDef) {
            hKey = new HKey(customerRowDef, row.get(o_cid1), row.get(o_cid2),
                            orderRowDef, row.get(o_oid1), row.get(o_oid2));
        } else if (rowDef == itemRowDef) {
            hKey = new HKey(customerRowDef, parent == null ? null : parent.get(o_cid1), parent == null ? null : parent.get(o_cid2),
                            orderRowDef, row.get(i_oid1), row.get(i_oid2),
                            itemRowDef, row.get(i_iid1), row.get(i_iid2));
        } else {
            fail();
        }
        row.parent(parent);
        return hKey;
    }

    private TestRow copyRow(TestRow row)
    {
        TestRow copy = new TestRow(row.getTableId(), store());
        for (Map.Entry<Integer, Object> entry : row.getFields().entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        copy.parent(row.parent());
        copy.hKey(hKey(row, row.parent()));
        return copy;
    }

    private static final String F = "xxxxx"; // F is for FILLER

    private TestStore testStore;
}
