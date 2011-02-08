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
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.akiban.cserver.itests.keyupdate.Schema.*;
import static junit.framework.Assert.*;

// Like KeyUpdateIT, but with cascading keys

public class KeyUpdate2IT extends ApiTestBase
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
        updateRow(newRow, i_oid, 0L);
        dbUpdate(oldRow, newRow);
        checkDB();
    }

    @Test
    public void testItemPKUpdate() throws Exception
    {
        // Set item.iid = 0 for item 222
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        updateRow(newRow, i_iid, 0L);
        dbUpdate(oldRow, newRow);
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
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
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
        dbUpdate(oldOrderRow, newOrderRow);
        checkDB();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 2
        TestRow oldCustomerRow = testStore.find(new HKey(customerRowDef, 2L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_cid, 0L);
        dbUpdate(oldCustomerRow, newCustomerRow);
        checkDB();
    }

    private void createSchema() throws InvalidOperationException
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
                              "primary key(cid, oid)",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_cid = 0;
        o_oid = 1;
        o_ox = 2;
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
        // For a schema with cascading keys, all PK indexes are hkey equivalent, so there's nothing else
        // to check.
        // TODO: Secondary indexes
    }

    private void populateTables() throws Exception
    {
        dbInsert(row(customerRowDef, 1, 100));
        dbInsert(row(orderRowDef, 1, 11, 1100));
        dbInsert(row(itemRowDef, 1, 11, 111, 11100));
        dbInsert(row(itemRowDef, 1, 11, 112, 11200));
        dbInsert(row(itemRowDef, 1, 11, 113, 11300));
        dbInsert(row(orderRowDef, 1, 12, 1200));
        dbInsert(row(itemRowDef, 1, 12, 121, 12100));
        dbInsert(row(itemRowDef, 1, 12, 122, 12200));
        dbInsert(row(itemRowDef, 1, 12, 123, 12300));
        dbInsert(row(orderRowDef, 1, 13, 1300));
        dbInsert(row(itemRowDef, 1, 13, 131, 13100));
        dbInsert(row(itemRowDef, 1, 13, 132, 13200));
        dbInsert(row(itemRowDef, 1, 13, 133, 13300));
        dbInsert(row(customerRowDef, 2, 200));
        dbInsert(row(orderRowDef, 2, 21, 2100));
        dbInsert(row(itemRowDef, 2, 21, 211, 21100));
        dbInsert(row(itemRowDef, 2, 21, 212, 21200));
        dbInsert(row(itemRowDef, 2, 21, 213, 21300));
        dbInsert(row(orderRowDef, 2, 22, 2200));
        dbInsert(row(itemRowDef, 2, 22, 221, 22100));
        dbInsert(row(itemRowDef, 2, 22, 222, 22200));
        dbInsert(row(itemRowDef, 2, 22, 223, 22300));
        dbInsert(row(orderRowDef, 2, 23, 2300));
        dbInsert(row(itemRowDef, 2, 23, 231, 23100));
        dbInsert(row(itemRowDef, 2, 23, 232, 23200));
        dbInsert(row(itemRowDef, 2, 23, 233, 23300));
        dbInsert(row(customerRowDef, 3, 300));
        dbInsert(row(orderRowDef, 3, 31, 3100));
        dbInsert(row(itemRowDef, 3, 31, 311, 31100));
        dbInsert(row(itemRowDef, 3, 31, 312, 31200));
        dbInsert(row(itemRowDef, 3, 31, 313, 31300));
        dbInsert(row(orderRowDef, 3, 32, 3200));
        dbInsert(row(itemRowDef, 3, 32, 321, 32100));
        dbInsert(row(itemRowDef, 3, 32, 322, 32200));
        dbInsert(row(itemRowDef, 3, 32, 323, 32300));
        dbInsert(row(orderRowDef, 3, 33, 3300));
        dbInsert(row(itemRowDef, 3, 33, 331, 33100));
        dbInsert(row(itemRowDef, 3, 33, 332, 33200));
        dbInsert(row(itemRowDef, 3, 33, 333, 33300));
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
            hKey = new HKey(customerRowDef, row.get(i_cid),
                            orderRowDef, row.get(i_oid),
                            itemRowDef, row.get(i_iid));
        } else {
            assertTrue(false);
        }
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
