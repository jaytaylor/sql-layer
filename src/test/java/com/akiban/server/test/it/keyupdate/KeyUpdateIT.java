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

import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.util.Tap;
import com.akiban.util.TapReport;
import org.junit.Test;

import java.util.*;

import static com.akiban.server.test.it.keyupdate.Schema.*;
import static junit.framework.Assert.*;
import static junit.framework.Assert.assertEquals;

public class KeyUpdateIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid = o for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, i_oid, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(oldRow, newRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(newRow, oldRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
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
        startMonitoringHKeyPropagation();
        dbUpdate(oldRow, newRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(newRow, oldRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
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
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
        checkInitialState();
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
        startMonitoringHKeyPropagation();
        dbUpdate(oldOrderRow, newOrderRow);
        checkHKeyPropagation(2, 6); // 2: 2 calls, 6: 3 items on each call
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 0L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, oldOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(newOrderRow, oldOrderRow);
        checkHKeyPropagation(2, 6);
        checkDB();
        checkInitialState();
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
        startMonitoringHKeyPropagation();
        dbUpdate(oldOrderRow, newOrderRow);
        checkHKeyPropagation(2, 3); // 3: 3 items become orphans. Not 6, because with oid 0, there are no affected items.
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, null, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, oldOrderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(newOrderRow, oldOrderRow);
        checkHKeyPropagation(2, 3);
        checkDB();
        checkInitialState();
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
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
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
        startMonitoringHKeyPropagation();
        dbUpdate(oldCustomerRow, newCustomerRow);
        // 9: customers and orders contain their own hkeys, but items do not. So only items will actually
        // be deleted/reinserted on hkey propagation. Customer 2 has 9 items.
        checkHKeyPropagation(2, 9);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(newCustomerRow, oldCustomerRow);
        checkHKeyPropagation(2, 9);
        checkDB();
        checkInitialState();
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
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
    }

    @Test
    public void testItemDelete() throws Exception
    {
        TestRow itemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        startMonitoringHKeyPropagation();
        dbDelete(itemRow);
        checkHKeyPropagation(1, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(itemRow);
        checkHKeyPropagation(1, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderDelete() throws Exception
    {
        TestRow orderRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbDelete(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, null, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, orderRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbInsert(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerDelete() throws Exception
    {
        TestRow customerRow = testStore.find(new HKey(customerRowDef, 2L));
        startMonitoringHKeyPropagation();
        dbDelete(customerRow);
        // Why 9: Only items do not contain their own hkeys, and so hkeyPropagateDown deletes/reinserts them.
        // Customer 2 has 9 items.
        checkHKeyPropagation(1, 9);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(customerRow);
        checkHKeyPropagation(1, 9);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testHKeyChangePropagations() throws Exception
    {
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 1L));
        TestRow newRow = copyRow(oldRow);
        newRow.put(0, 99);
        startMonitoringHKeyPropagation();
/*
        Tap.setEnabled(".*propagate$", true);
        Tap.reset(".*propagate$");
*/
        dbUpdate(oldRow, newRow);
        checkHKeyPropagation(2, 9);
/*
        TapReport[] tapReports = Tap.getReport(".*propagate_hkey_change$");
        assertEquals(1, tapReports.length);
        TapReport propagateTap = tapReports[0];
        // There should be two propagations, one for deletion of the old row, and one for insertion of the new row
        assertEquals(2, propagateTap.getInCount());
*/
    }


    @Override
    protected void createSchema() throws InvalidOperationException
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
                              "priority int",
                              "when int",
                              "key(priority)",
                              "unique(when)",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_oid = 0;
        o_cid = 1;
        o_ox = 2;
        o_priority = 3;
        o_when = 4;
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

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRowDef, o_oid, o_cid);
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, itemRowDef, i_iid, HKeyElement.from(1), i_oid);
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRowDef, o_priority, o_cid, o_oid);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRowDef, o_when, o_cid, o_oid);
    }

    @Override
    protected void populateTables() throws Exception
    {
        TestRow order;
        //                               HKey reversed, value
        dbInsert(     row(customerRowDef,          1,   100));
        dbInsert(order = row(orderRowDef,      11, 1,   1100,   81, 9001));
        dbInsert(  row(order, itemRowDef, 111, 11,      11100));
        dbInsert(  row(order, itemRowDef, 112, 11,      11200));
        dbInsert(  row(order, itemRowDef, 113, 11,      11300));
        dbInsert(order = row(orderRowDef,      12, 1,   1200,   83, 9002));
        dbInsert(  row(order, itemRowDef, 121, 12,      12100));
        dbInsert(  row(order, itemRowDef, 122, 12,      12200));
        dbInsert(  row(order, itemRowDef, 123, 12,      12300));
        dbInsert(order = row(orderRowDef,      13, 1,   1300,   81, 9003));
        dbInsert(  row(order, itemRowDef, 131, 13,      13100));
        dbInsert(  row(order, itemRowDef, 132, 13,      13200));
        dbInsert(  row(order, itemRowDef, 133, 13,      13300));

        dbInsert(row(customerRowDef,               2,   200));
        dbInsert(order = row(orderRowDef,      21, 2,   2100,   83, 9004));
        dbInsert(  row(order, itemRowDef, 211, 21,      21100));
        dbInsert(  row(order, itemRowDef, 212, 21,      21200));
        dbInsert(  row(order, itemRowDef, 213, 21,      21300));
        dbInsert(order = row(orderRowDef,      22, 2,   2200,   81, 9005));
        dbInsert(  row(order, itemRowDef, 221, 22,      22100));
        dbInsert(  row(order, itemRowDef, 222, 22,      22200));
        dbInsert(  row(order, itemRowDef, 223, 22,      22300));
        dbInsert(order = row(orderRowDef,      23, 2,   2300,   82, 9006));
        dbInsert(  row(order, itemRowDef, 231, 23,      23100));
        dbInsert(  row(order, itemRowDef, 232, 23,      23200));
        dbInsert(  row(order, itemRowDef, 233, 23,      23300));

        dbInsert(row(customerRowDef,               3,   300));
        dbInsert(order = row(orderRowDef,      31, 3,   3100,   81, 9007));
        dbInsert(  row(order, itemRowDef, 311, 31,      31100));
        dbInsert(  row(order, itemRowDef, 312, 31,      31200));
        dbInsert(  row(order, itemRowDef, 313, 31,      31300));
        dbInsert(order = row(orderRowDef,      32, 3,   3200,   82, 9008));
        dbInsert(  row(order, itemRowDef, 321, 32,      32100));
        dbInsert(  row(order, itemRowDef, 322, 32,      32200));
        dbInsert(  row(order, itemRowDef, 323, 32,      32300));
        dbInsert(order = row(orderRowDef,      33, 3,   3300,   83, 9009));
        dbInsert(  row(order, itemRowDef, 331, 33,      33100));
        dbInsert(  row(order, itemRowDef, 332, 33,      33200));
        dbInsert(  row(order, itemRowDef, 333, 33,      33300));
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

    @Override
    protected HKey hKey(TestRow row)
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
    
    @Override
    protected boolean checkChildPKs() {
        return true;
    }

    @Override
    protected HKey hKey(TestRow row, TestRow parent)
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
}
