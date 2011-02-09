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

package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.message.ErrorCode;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.akiban.cserver.itests.keyupdate.Schema.*;
import static junit.framework.Assert.*;

public class KeyUpdateIT extends ApiTestBase
{
    @Before
    public void before() throws Exception
    {
        testStore = new TestStore(persistitStore());
        createSchema();
        populateTables();
    }

    @Test
    public void testInitialState() throws Exception
    {
        checkDB();
    }

    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid = o for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, i_oid, 0L, null);
        dbUpdate(oldRow, newRow);
        checkDB();
    }

    @Test
    public void testItemPKUpdate() throws Exception
    {
        // Set item.iid = 0 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        assertNotNull(parent);
        updateRow(newRow, i_iid, 0L, parent);
        dbUpdate(oldRow, newRow);
        checkDB();
    }

    @Test
    public void testItemPKUpdateCreatingDuplicate() throws Exception
    {
        // Set item.iid = 223 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        assertNotNull(parent);
        updateRow(newRow, i_iid, 223L, parent);
        try {
            dbUpdate(oldRow, newRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testOrderFKUpdate() throws Exception
    {
        // Set order.cid = 0 for order 22
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_cid, 0L, null);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, newOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 22
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_oid, 0L, null);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 21 for order 22
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        TestRow newRow = copyRow(oldRow);
        TestRow parent = testStore.find(new HKey(customerRowDef, 2L));
        assertNotNull(parent);
        updateRow(newRow, o_oid, 21L, parent);
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
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 2L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid, 0L, null);
        dbUpdate(oldCustomerRow, newCustomerRow);
        checkDB();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 1 for customer 3
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 3L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid, 1L, null);
        try {
            dbUpdate(oldCustomerRow, newCustomerRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    private void createSchema() throws InvalidOperationException
    {
        // customer
        customerId = createTable("coi", "customer",
                                 "cid int not null key",
                                 "cx int");
        c_cid = 0;
        c_cx = 1;
        // order
        orderId = createTable("coi", "order",
                              "oid int not null key",
                              "cid int",
                              "ox int",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_oid = 0;
        o_cid = 1;
        o_ox = 2;
        // item
        itemId = createTable("coi", "item",
                             "iid int not null key",
                             "oid int",
                             "ix int",
                             "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        i_iid = 0;
        i_oid = 1;
        i_ix = 2;
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
        // Records
        RecordCollectingTreeRecordVisistor testVisitor = new RecordCollectingTreeRecordVisistor();
        RecordCollectingTreeRecordVisistor realVisitor = new RecordCollectingTreeRecordVisistor();
        testStore.traverse(session, groupRowDef, testVisitor, realVisitor);
        assertEquals(testVisitor.records(), realVisitor.records());
        // Check indexes
        RecordCollectingIndexRecordVisistor indexVisitor;
        // Customer PK index - skip. This index is hkey equivalent, and we've already checked the full records.
        // Order PK index
        indexVisitor = new RecordCollectingIndexRecordVisistor();
        testStore.traverse(session, orderRowDef.getPKIndexDef(), indexVisitor);
        assertEquals(orderPKIndex(testVisitor.records()), indexVisitor.records());
        // Item PK index
        indexVisitor = new RecordCollectingIndexRecordVisistor();
        testStore.traverse(session, itemRowDef.getPKIndexDef(), indexVisitor);
        assertEquals(itemPKIndex(testVisitor.records()), indexVisitor.records());
    }

    private List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        List<List<Object>> indexEntries = new ArrayList<List<Object>>();
        for (TreeRecord record : records) {
            if (record.row().getRowDef() == orderRowDef) {
                List<Object> indexEntry =
                    Arrays.asList(record.row().get(o_oid),
                                  record.row().get(o_cid));
                indexEntries.add(indexEntry);
            }
        }
        Collections.sort(indexEntries,
                         new Comparator<List<Object>>()
                         {
                             @Override
                             public int compare(List<Object> x, List<Object> y)
                             {
                                 // compare oids
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
                    Arrays.asList(record.row().get(i_iid), // iid
                                  record.hKey().objectArray()[1], // cid
                                  record.row().get(i_oid)); // oid
                indexEntries.add(indexEntry);
            }
        }
        Collections.sort(indexEntries,
                         new Comparator<List<Object>>()
                         {
                             @Override
                             public int compare(List<Object> x, List<Object> y)
                             {
                                 // compare iids
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
        dbInsert(row(customerRowDef, 1, 100));
        dbInsert(order = row(orderRowDef, 11, 1, 1100));
        dbInsert(row(order, itemRowDef, 111, 11, 11100));
        dbInsert(row(order, itemRowDef, 112, 11, 11200));
        dbInsert(row(order, itemRowDef, 113, 11, 11300));
        dbInsert(order = row(orderRowDef, 12, 1, 1200));
        dbInsert(row(order, itemRowDef, 121, 12, 12100));
        dbInsert(row(order, itemRowDef, 122, 12, 12200));
        dbInsert(row(order, itemRowDef, 123, 12, 12300));
        dbInsert(order = row(orderRowDef, 13, 1, 1300));
        dbInsert(row(order, itemRowDef, 131, 13, 13100));
        dbInsert(row(order, itemRowDef, 132, 13, 13200));
        dbInsert(row(order, itemRowDef, 133, 13, 13300));
        dbInsert(row(customerRowDef, 2, 200));
        dbInsert(order = row(orderRowDef, 21, 2, 2100));
        dbInsert(row(order, itemRowDef, 211, 21, 21100));
        dbInsert(row(order, itemRowDef, 212, 21, 21200));
        dbInsert(row(order, itemRowDef, 213, 21, 21300));
        dbInsert(order = row(orderRowDef, 22, 2, 2200));
        dbInsert(row(order, itemRowDef, 221, 22, 22100));
        dbInsert(row(order, itemRowDef, 222, 22, 22200));
        dbInsert(row(order, itemRowDef, 223, 22, 22300));
        dbInsert(order = row(orderRowDef, 23, 2, 2300));
        dbInsert(row(order, itemRowDef, 231, 23, 23100));
        dbInsert(row(order, itemRowDef, 232, 23, 23200));
        dbInsert(row(order, itemRowDef, 233, 23, 23300));
        dbInsert(row(customerRowDef, 3, 300));
        dbInsert(order = row(orderRowDef, 31, 3, 3100));
        dbInsert(row(order, itemRowDef, 311, 31, 31100));
        dbInsert(row(order, itemRowDef, 312, 31, 31200));
        dbInsert(row(order, itemRowDef, 313, 31, 31300));
        dbInsert(order = row(orderRowDef, 32, 3, 3200));
        dbInsert(row(order, itemRowDef, 321, 32, 32100));
        dbInsert(row(order, itemRowDef, 322, 32, 32200));
        dbInsert(row(order, itemRowDef, 323, 32, 32300));
        dbInsert(order = row(orderRowDef, 33, 3, 3300));
        dbInsert(row(order, itemRowDef, 331, 33, 33100));
        dbInsert(row(order, itemRowDef, 332, 33, 33200));
        dbInsert(row(order, itemRowDef, 333, 33, 33300));
    }

    private TestRow row(RowDef table, Object... values)
    {
        TestRow row = new TestRow(table.getRowDefId());
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
        TestRow row = new TestRow(table.getRowDefId());
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

    private void dbInsert(TestRow row) throws Exception
    {
        testStore.writeRow(session, row);
    }

    private void dbUpdate(TestRow oldRow, TestRow newRow) throws Exception
    {
        testStore.updateRow(session, oldRow, newRow, null);
    }

    private HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            hKey = new HKey(customerRowDef, row.get(c_cid));
        } else if (rowDef == orderRowDef) {
            hKey = new HKey(customerRowDef, row.get(o_cid),
                            orderRowDef, row.get(o_oid));
        } else if (rowDef == itemRowDef) {
            assertNotNull(row.parent());
            hKey = new HKey(customerRowDef, row.parent().get(o_cid),
                            orderRowDef, row.get(i_oid),
                            itemRowDef, row.get(i_iid));
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
            hKey = new HKey(customerRowDef, row.get(c_cid));
        } else if (rowDef == orderRowDef) {
            hKey = new HKey(customerRowDef, row.get(o_cid),
                            orderRowDef, row.get(o_oid));
        } else if (rowDef == itemRowDef) {
            hKey = new HKey(customerRowDef, parent == null ? null : parent.get(o_cid),
                            orderRowDef, row.get(i_oid),
                            itemRowDef, row.get(i_iid));
        } else {
            fail();
        }
        row.parent(parent);
        return hKey;
    }

    private TestRow copyRow(TestRow row)
    {
        TestRow copy = new TestRow(row.getTableId());
        for (Map.Entry<Integer, Object> entry : row.getFields().entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        copy.parent(row.parent());
        copy.hKey(hKey(row));
        return copy;
    }

    private TestStore testStore;
}
