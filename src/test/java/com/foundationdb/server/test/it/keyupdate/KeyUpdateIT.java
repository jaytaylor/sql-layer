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

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;
import org.junit.Test;

import java.util.List;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.*;

// This test uses a 4-level group: the COI schema with a Vendor table that is the parent of Customer.
// hkey maintenance for the leaf of this group is unlike that of a 3-table group because part of the hkey
// comes from a grandparent, not a parent.

public class KeyUpdateIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid = 0 for item 1222
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_oid, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(originalItem, updatedItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedItem, originalItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdate() throws Exception
    {
        // Set item.iid = 0 for item 1222
        TestRow order = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L));
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        assertNotNull(order);
        updateRow(updatedItem, i_iid, 0L, order);
        startMonitoringHKeyPropagation();
        dbUpdate(originalItem, updatedItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedItem, originalItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdateCreatingDuplicate() throws Exception
    {
        // Set item.iid = 1223 for item 1222
        TestRow order = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L));
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        assertNotNull(order);
        updateRow(updatedItem, i_iid, 1223L, order);
        try {
            dbUpdate(originalItem, updatedItem);
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
        // Set order.cid = 0 for order 222
        TestRow customer = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_cid, 0L, null);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L, itemRD, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, updatedOrder));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 6);
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, null, customerRD, 0L, orderRD, 222L, itemRD, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder, customer));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedOrder, originalOrder);
        checkHKeyPropagation(2, 6);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 222
        TestRow customer = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow udpatedOrder = copyRow(originalOrder);
        updateRow(udpatedOrder, o_oid, 0L, customer);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L, itemRD, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, udpatedOrder);
        checkHKeyPropagation(2, 3); // 3: 3 items become orphans, no items are adopted orphans.
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, null, customerRD, null, orderRD, 222L, itemRD, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder, customer));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedOrder, originalOrder);
        checkHKeyPropagation(2, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 221 for order 222
        TestRow customer = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        assertNotNull(customer);
        updateRow(updatedOrder, o_oid, 221L, customer);
        try {
            dbUpdate(originalOrder, updatedOrder);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
    }

    @Test
    public void testCustomerFKUpdate() throws Exception
    {
        // Set customer.vid = 0 for customer 13
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        TestRow udpatedCustomer = copyRow(originalCustomer);
        updateRow(udpatedCustomer, c_vid, 0L, null);
        // Propagate change to customer 13s descendents to reflect db state
        for (long oid = 131; oid <= 133; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, udpatedCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, udpatedCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, udpatedCustomer);
        checkHKeyPropagation(2, 24); // 3 orders, 3 items per order, delete/reinsert each
        checkDB();
        // Revert change
        for (long oid = 131; oid <= 133; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 0L, customerRD, 13L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 0L, customerRD, 13L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 24);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 22
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_cid, 0L, null);
        // Propagate change to customer 22's orders and items to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 12); // 3 orders, 3 items per order, delete, but no reinsert - there is no customer 0
        checkDB();
        // Revert change
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, null, customerRD, 22L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, null, customerRD, 22L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 12);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 11 for customer 23
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 2L, customerRD, 23L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_cid, 11L, null);
        try {
            dbUpdate(originalCustomer, updatedCustomer);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
    }

    @Test
    public void testVendorPKUpdate() throws Exception
    {
        // Set vendor.vid = 0 for vendor 1
        TestRow originalVendor = testStore.find(new HKey(vendorRD, 1L));
        TestRow updatedVendor = copyRow(originalVendor);
        updateRow(updatedVendor, v_vid, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(originalVendor, updatedVendor);
        checkHKeyPropagation(2, 0); // customer has vid, isn't affected by vendor update
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedVendor, originalVendor);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }
    
    @Test
    public void testVendorPKUpdateCreatingDuplicate() throws Exception
    {
        // Set vendor.vid = 2 for vendor 1
        TestRow originalVendorRow = testStore.find(new HKey(vendorRD, 1L));
        TestRow updatedVendorRow = copyRow(originalVendorRow);
        updateRow(updatedVendorRow, v_vid, 2L, null);
        try {
            dbUpdate(originalVendorRow, updatedVendorRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY,  e.getCode());
        }
        checkDB();
    }
    
    @Test
    public void testItemDelete() throws Exception
    {
        TestRow itemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L, itemRD, 2222L));
        startMonitoringHKeyPropagation();
        dbDelete(itemRow);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(itemRow);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderDelete() throws Exception
    {
        TestRow customerRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow orderRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L, itemRD, iid));
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
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, null, customerRD, null, orderRD, 222L, itemRD, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, orderRow, customerRow));
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
        TestRow customerRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        // Propagate change to customer's descendents to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbDelete(customerRow);
        // 12; 3 orders, with 3 items each
        checkHKeyPropagation(1, 12);
        checkDB();
        // Revert change
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, null, customerRD, 22L, orderRD, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, customerRow));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, null, customerRD, 22L, orderRD, oid, itemRD, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, customerRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbInsert(customerRow);
        checkHKeyPropagation(1, 12);
        checkDB();
        checkInitialState();
    }
    
    @Test
    public void testVendorDelete() throws Exception
    {
        TestRow vendorRow = testStore.find(new HKey(vendorRD, 1L));
        startMonitoringHKeyPropagation();
        dbDelete(vendorRow);
        // TODO: Why not apply the PDG optimization on inserts and deletes?
        checkHKeyPropagation(1, 39);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(vendorRow);
        checkHKeyPropagation(1, 39);
        checkDB();
        checkInitialState();
    }

    @Override
    protected void createSchema() throws InvalidOperationException
    {
        // vendor
        vendorId = createTable("coi", "vendor",
                               "vid bigint not null primary key",
                               "vx bigint");
        v_vid = 0;
        v_vx = 1;
        // customer
        customerId = createTable("coi", "customer",
                                 "cid bigint not null primary key",
                                 "vid bigint",
                                 "cx bigint",
                                 "grouping foreign key (vid) references vendor(vid)");
        c_cid = 0;
        c_vid = 1;
        c_cx = 2;
        // order
        orderId = createTable("coi", "order",
                              "oid bigint not null primary key",
                              "cid bigint",
                              "ox bigint",
                              "priority bigint",
                              "when bigint",
                              "unique(when)",
                              "grouping foreign key (cid) references customer(cid)");
        createIndex("coi", "order", "priority", "priority");
        o_oid = 0;
        o_cid = 1;
        o_ox = 2;
        o_priority = 3;
        o_when = 4;
        // item
        itemId = createTable("coi", "item",
                             "iid bigint not null primary key",
                             "oid bigint",
                             "ix bigint",
                             "grouping foreign key (oid) references \"order\"(oid)");
        i_iid = 0;
        i_oid = 1;
        i_ix = 2;
        // group
        vendorRD = getRowDef(vendorId);
        customerRD = getRowDef(customerId);
        orderRD = getRowDef(orderId);
        itemRD = getRowDef(itemId);
        group = customerRD.getGroup();
    }

    @Override
    protected List<List<Object>> vendorPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, vendorRD, v_vid);
    }

    @Override
    protected List<List<Object>> customerPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, customerRD, c_cid, c_vid);
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD, o_oid, HKeyElement.from(1), o_cid);
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, itemRD, i_iid, HKeyElement.from(1), HKeyElement.from(3), i_oid);
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD, o_priority, HKeyElement.from(1), o_cid, o_oid);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD, o_when, HKeyElement.from(1), o_cid, o_oid);
    }

    @Override
    protected void populateTables() throws Exception
    {
        // non-key vendor fields
        //     vx = vid * 100
        // non-key customer fields
        //     cx = cid * 100
        // non-key order fields
        //     ox = oid * 100
        //     priority = 8[1-3]
        //     when = unique, start counting at 9001
        // non-key item fields
        //     ix = iid * 100
        TestRow customer;
        TestRow order;
        //                               HKey reversed, value
        // Vendor 1
        dbInsert(             row(vendorRD,                                   1,   100));
        //
        dbInsert(customer =   row(customerRD,                            11,  1,   1100));
        dbInsert(order =      row(customer, orderRD,                111, 11,       11100,   81, 9001));
        dbInsert(             row(order, customer, itemRD,    1111, 111,           111100));
        dbInsert(             row(order, customer, itemRD,    1112, 111,           111200));
        dbInsert(             row(order, customer, itemRD,    1113, 111,           111300));
        dbInsert(order =      row(customer, orderRD,                112, 11,       11200,   83, 9002));
        dbInsert(             row(order, customer, itemRD,    1121, 112,           112100));
        dbInsert(             row(order, customer, itemRD,    1122, 112,           112200));
        dbInsert(             row(order, customer, itemRD,    1123, 112,           112300));
        dbInsert(order =      row(customer, orderRD,                113, 11,       11300,   81, 9003));
        dbInsert(             row(order, customer, itemRD,    1131, 113,           113100));
        dbInsert(             row(order, customer, itemRD,    1132, 113,           113200));
        dbInsert(             row(order, customer, itemRD,    1133, 113,           113300));

        dbInsert(customer =   row(customerRD,                            12,  1,   1200));
        dbInsert(order =      row(customer, orderRD,                121, 12,       12100,   83, 9004));
        dbInsert(             row(order, customer, itemRD,    1211, 121,           121100));
        dbInsert(             row(order, customer, itemRD,    1212, 121,           121200));
        dbInsert(             row(order, customer, itemRD,    1213, 121,           121300));
        dbInsert(order =      row(customer, orderRD,                122, 12,       12200,   81, 9005));
        dbInsert(             row(order, customer, itemRD,    1221, 122,           122100));
        dbInsert(             row(order, customer, itemRD,    1222, 122,           122200));
        dbInsert(             row(order, customer, itemRD,    1223, 122,           122300));
        dbInsert(order =      row(customer, orderRD,                123, 12,       12300,   82, 9006));
        dbInsert(             row(order, customer, itemRD,    1231, 123,           123100));
        dbInsert(             row(order, customer, itemRD,    1232, 123,           123200));
        dbInsert(             row(order, customer, itemRD,    1233, 123,           123300));

        dbInsert(customer =   row(customerRD,                            13,  1,   1300));
        dbInsert(order =      row(customer, orderRD,                131, 13,       13100,   82, 9007));
        dbInsert(             row(order, customer, itemRD,    1311, 131,           131100));
        dbInsert(             row(order, customer, itemRD,    1312, 131,           131200));
        dbInsert(             row(order, customer, itemRD,    1313, 131,           131300));
        dbInsert(order =      row(customer, orderRD,                132, 13,       13200,   83, 9008));
        dbInsert(             row(order, customer, itemRD,    1321, 132,           132100));
        dbInsert(             row(order, customer, itemRD,    1322, 132,           132200));
        dbInsert(             row(order, customer, itemRD,    1323, 132,           132300));
        dbInsert(order =      row(customer, orderRD,                133, 13,       13300,   81, 9009));
        dbInsert(             row(order, customer, itemRD,    1331, 133,           133100));
        dbInsert(             row(order, customer, itemRD,    1332, 133,           133200));
        dbInsert(             row(order, customer, itemRD,    1333, 133,           133300));
        //
        // Vendor 2
        dbInsert(             row(vendorRD,                                   2,   200));
        //
        dbInsert(customer =   row(customerRD,                            21,  2,   2100));
        dbInsert(order =      row(customer, orderRD,                211, 21,       21100,   81, 9010));
        dbInsert(             row(order, customer, itemRD,    2111, 211,           211100));
        dbInsert(             row(order, customer, itemRD,    2112, 211,           211200));
        dbInsert(             row(order, customer, itemRD,    2113, 211,           211300));
        dbInsert(order =      row(customer, orderRD,                212, 21,       21200,   83, 9011));
        dbInsert(             row(order, customer, itemRD,    2121, 212,           212100));
        dbInsert(             row(order, customer, itemRD,    2122, 212,           212200));
        dbInsert(             row(order, customer, itemRD,    2123, 212,           212300));
        dbInsert(order =      row(customer, orderRD,                213, 21,       21300,   82, 9012));
        dbInsert(             row(order, customer, itemRD,    2131, 213,           213100));
        dbInsert(             row(order, customer, itemRD,    2132, 213,           213200));
        dbInsert(             row(order, customer, itemRD,    2133, 213,           213300));

        dbInsert(customer =   row(customerRD,                            22,  2,   2200));
        dbInsert(order =      row(customer, orderRD,                221, 22,       22100,   82, 9013));
        dbInsert(             row(order, customer, itemRD,    2211, 221,           221100));
        dbInsert(             row(order, customer, itemRD,    2212, 221,           221200));
        dbInsert(             row(order, customer, itemRD,    2213, 221,           221300));
        dbInsert(order =      row(customer, orderRD,                222, 22,       22200,   82, 9014));
        dbInsert(             row(order, customer, itemRD,    2221, 222,           222100));
        dbInsert(             row(order, customer, itemRD,    2222, 222,           222200));
        dbInsert(             row(order, customer, itemRD,    2223, 222,           222300));
        dbInsert(order =      row(customer, orderRD,                223, 22,       22300,   81, 9015));
        dbInsert(             row(order, customer, itemRD,    2231, 223,           223100));
        dbInsert(             row(order, customer, itemRD,    2232, 223,           223200));
        dbInsert(             row(order, customer, itemRD,    2233, 223,           223300));

        dbInsert(customer =   row(customerRD,                            23,  2,   2300));
        dbInsert(order =      row(customer, orderRD,                231, 23,       23100,   82, 9016));
        dbInsert(             row(order, customer, itemRD,    2311, 231,           231100));
        dbInsert(             row(order, customer, itemRD,    2312, 231,           231200));
        dbInsert(             row(order, customer, itemRD,    2313, 231,           231300));
        dbInsert(order =      row(customer, orderRD,                232, 23,       23200,   83, 9017));
        dbInsert(             row(order, customer, itemRD,    2321, 232,           232100));
        dbInsert(             row(order, customer, itemRD,    2322, 232,           232200));
        dbInsert(             row(order, customer, itemRD,    2323, 232,           232300));
        dbInsert(order =      row(customer, orderRD,                233, 23,       23300,   81, 9018));
        dbInsert(             row(order, customer, itemRD,    2331, 233,           233100));
        dbInsert(             row(order, customer, itemRD,    2332, 233,           233200));
        dbInsert(             row(order, customer, itemRD,    2333, 233,           233300));
    }

    @Override
    protected HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            hKey = new HKey(vendorRD, row.get(v_vid));
        } else if (rowDef == customerRD) {
            hKey = new HKey(vendorRD, row.get(c_vid), 
                            customerRD, row.get(c_cid));
        } else if (rowDef == orderRD) {
            assertNotNull(row.parent());
            hKey = new HKey(vendorRD, row.parent().get(c_vid),
                            customerRD, row.get(o_cid),
                            orderRD, row.get(o_oid));
        } else if (rowDef == itemRD) {
            assertNotNull(row.parent());
            assertNotNull(row.parent().parent());
            hKey = new HKey(vendorRD, row.parent().parent().get(c_vid),
                            customerRD, row.parent().get(o_cid),
                            orderRD, row.get(i_oid),
                            itemRD, row.get(i_iid));
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
    protected HKey hKey(TestRow row, TestRow parent, TestRow grandparent)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            hKey = new HKey(vendorRD, row.get(v_vid));
        } else if (rowDef == customerRD) {
            hKey = new HKey(vendorRD, row.get(c_vid),
                            customerRD, row.get(c_cid));
        } else if (rowDef == orderRD) {
            hKey = new HKey(vendorRD, parent == null ? null : parent.get(c_vid),
                            customerRD, row.get(o_cid),
                            orderRD, row.get(o_oid));
        } else if (rowDef == itemRD) {
            hKey = new HKey(vendorRD, grandparent == null ? null : grandparent.get(c_vid),
                            customerRD, parent == null ? null : parent.get(o_cid),
                            orderRD, row.get(i_oid),
                            itemRD, row.get(i_iid));
        } else {
            fail();
        }
        row.parent(parent);
        return hKey;
    }

    protected void confirmColumns()
    {
        confirmColumn(vendorRD, v_vid, "vid");
        confirmColumn(vendorRD, v_vx, "vx");

        confirmColumn(customerRD, c_cid, "cid");
        confirmColumn(customerRD, c_vid, "vid");
        confirmColumn(customerRD, c_cx, "cx");

        confirmColumn(orderRD, o_oid, "oid");
        confirmColumn(orderRD, o_cid, "cid");
        confirmColumn(orderRD, o_ox, "ox");
        confirmColumn(orderRD, o_priority, "priority");
        confirmColumn(orderRD, o_when, "when");

        confirmColumn(itemRD, i_iid, "iid");
        confirmColumn(itemRD, i_oid, "oid");
        confirmColumn(itemRD, i_ix, "ix");
    }
}
