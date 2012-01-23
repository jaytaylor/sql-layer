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
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Test;

import java.util.List;

import static com.akiban.server.test.it.keyupdate.Schema.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

// Like KeyUpdateIT, but with cascading keys

public class KeyUpdateCascadingKeysIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid = o for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, i_oid, 0L);
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
        updateRow(newRow, i_iid, 0L);
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
        updateRow(newRow, i_iid, 223L);
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
        updateRow(newOrderRow, o_cid, 0L);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(oldOrderRow, newOrderRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(newOrderRow, oldOrderRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 22
        TestRow oldOrderRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        TestRow newOrderRow = copyRow(oldOrderRow);
        updateRow(newOrderRow, o_oid, 0L);
        // Propagate change to order 22's items
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(oldOrderRow, newOrderRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(newOrderRow, oldOrderRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 21 for order 22
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, o_oid, 21L);
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
        updateRow(newCustomerRow, c_cid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(oldCustomerRow, newCustomerRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(newCustomerRow, oldCustomerRow);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 1 for customer 3
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 3L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid, 1L);
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
            newItemRow.hKey(hKey(newItemRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbDelete(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        // Revert change
        for (long iid = 221; iid <= 223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow));
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
        checkHKeyPropagation(1, 12);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(customerRow);
        checkHKeyPropagation(1, 12);
        checkDB();
        checkInitialState();
    }
    
    @Override
    protected void createSchema() throws InvalidOperationException
    {
        // customer
        customerId = createTable("coi", "customer",
                                 "cid int not null",
                                 "cx int",
                                 "primary key(cid)");
        c_cid = 0;
        c_cx = 1;
        // order
        orderId = createTable("coi", "order",
                              "cid int not null",
                              "oid int not null",
                              "ox int",
                              "priority int",
                              "when int",
                              "primary key(cid, oid)",
                              "key(priority)",
                              "unique(when)",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_cid = 0;
        o_oid = 1;
        o_ox = 2;
        o_priority = 3;
        o_when = 4;
        // item
        itemId = createTable("coi", "item",
                             "cid int not null",
                             "oid int not null",
                             "iid int not null",
                             "ix int",
                             "primary key(cid, oid, iid)",
                             "constraint __akiban_io foreign key __akiban_io(cid, oid) references order(cid, oid)");
        i_cid = 0;
        i_oid = 1;
        i_iid = 2;
        i_ix = 3;
        orderRowDef = rowDefCache().getRowDef(orderId);
        customerRowDef = rowDefCache().getRowDef(customerId);
        itemRowDef = rowDefCache().getRowDef(itemId);
        // group
        int groupRowDefId = customerRowDef.getGroupRowDefId();
        groupRowDef = store().getRowDefCache().getRowDef(groupRowDefId);
    }

    private void updateRow(TestRow row, int column, Object newValue)
    {
        row.put(column, newValue);
        row.hKey(hKey(row));
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
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
        dbInsert(row(customerRowDef, 1, 100));
        dbInsert(row(orderRowDef,    1, 11, 1100, 81, 9001));
        dbInsert(row(itemRowDef,     1, 11, 111, 11100));
        dbInsert(row(itemRowDef,     1, 11, 112, 11200));
        dbInsert(row(itemRowDef,     1, 11, 113, 11300));
        dbInsert(row(orderRowDef,    1, 12, 1200, 83, 9002));
        dbInsert(row(itemRowDef,     1, 12, 121, 12100));
        dbInsert(row(itemRowDef,     1, 12, 122, 12200));
        dbInsert(row(itemRowDef,     1, 12, 123, 12300));
        dbInsert(row(orderRowDef,    1, 13, 1300, 81, 9003));
        dbInsert(row(itemRowDef,     1, 13, 131, 13100));
        dbInsert(row(itemRowDef,     1, 13, 132, 13200));
        dbInsert(row(itemRowDef,     1, 13, 133, 13300));

        dbInsert(row(customerRowDef, 2, 200));
        dbInsert(row(orderRowDef,    2, 21, 2100, 83, 9004));
        dbInsert(row(itemRowDef,     2, 21, 211, 21100));
        dbInsert(row(itemRowDef,     2, 21, 212, 21200));
        dbInsert(row(itemRowDef,     2, 21, 213, 21300));
        dbInsert(row(orderRowDef,    2, 22, 2200, 81, 9005));
        dbInsert(row(itemRowDef,     2, 22, 221, 22100));
        dbInsert(row(itemRowDef,     2, 22, 222, 22200));
        dbInsert(row(itemRowDef,     2, 22, 223, 22300));
        dbInsert(row(orderRowDef,    2, 23, 2300, 82, 9006));
        dbInsert(row(itemRowDef,     2, 23, 231, 23100));
        dbInsert(row(itemRowDef,     2, 23, 232, 23200));
        dbInsert(row(itemRowDef,     2, 23, 233, 23300));

        dbInsert(row(customerRowDef, 3, 300));
        dbInsert(row(orderRowDef,    3, 31, 3100, 81, 9007));
        dbInsert(row(itemRowDef,     3, 31, 311, 31100));
        dbInsert(row(itemRowDef,     3, 31, 312, 31200));
        dbInsert(row(itemRowDef,     3, 31, 313, 31300));
        dbInsert(row(orderRowDef,    3, 32, 3200, 82, 9008));
        dbInsert(row(itemRowDef,     3, 32, 321, 32100));
        dbInsert(row(itemRowDef,     3, 32, 322, 32200));
        dbInsert(row(itemRowDef,     3, 32, 323, 32300));
        dbInsert(row(orderRowDef,    3, 33, 3300, 83, 9009));
        dbInsert(row(itemRowDef,     3, 33, 331, 33100));
        dbInsert(row(itemRowDef,     3, 33, 332, 33200));
        dbInsert(row(itemRowDef,     3, 33, 333, 33300));
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
            hKey = new HKey(customerRowDef, row.get(i_cid),
                            orderRowDef, row.get(i_oid),
                            itemRowDef, row.get(i_iid));
        } else {
            fail();
        }
        return hKey;
    }

    @Override
    protected HKey hKey(TestRow row, TestRow ignored) {
        return hKey(row);
    }

    @Override
    protected boolean checkChildPKs() {
        return false;
    }
}
