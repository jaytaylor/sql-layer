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

import com.foundationdb.qp.rowtype.RowType;
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
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, customerRT, 12L, orderRT, 122L, itemRT, 1222L));
        KeyUpdateRow updatedItem = updateRow(originalItem, i_oid, 0L, null);
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
        KeyUpdateRow order = testStore.find(new HKey(vendorRT, 1L, customerRT, 12L, orderRT, 122L));
        assertNotNull(order);
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, customerRT, 12L, orderRT, 122L, itemRT, 1222L));
        KeyUpdateRow updatedItem = updateRow(originalItem, i_iid, 0L, order);
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
        KeyUpdateRow order = testStore.find(new HKey(vendorRT, 1L, customerRT, 12L, orderRT, 122L));
        assertNotNull(order);
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, customerRT, 12L, orderRT, 122L, itemRT, 1222L));
        KeyUpdateRow updatedItem = updateRow(originalItem, i_iid, 1223L, order);
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
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L));
        KeyUpdateRow updatedOrder = updateRow(originalOrder, o_cid, 0L, null);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, customerRT, 0L, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L));
        KeyUpdateRow updatedOrder = updateRow(originalOrder, o_oid, 0L, customer);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 3); // 3: 3 items become orphans, no items are adopted orphans.
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, customerRT, null, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder, customer));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedOrder, originalOrder);
        checkHKeyPropagation(2, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 221 for order 222
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        assertNotNull(customer);
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L));
        KeyUpdateRow updatedOrder = updateRow(originalOrder, o_oid, 221L, customer);
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
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 1L, customerRT, 13L));
        KeyUpdateRow updatedCustomer = updateRow(originalCustomer, c_vid, 0L, null);
        // Propagate change to customer 13s descendents to reflect db state
        for (long oid = 131; oid <= 133; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 1L, customerRT, 13L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, updatedCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 1L, customerRT, 13L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, updatedCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 24); // 3 orders, 3 items per order, delete/reinsert each
        checkDB();
        // Revert change
        for (long oid = 131; oid <= 133; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 0L, customerRT, 13L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 0L, customerRT, 13L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 24);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 22
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        KeyUpdateRow updatedCustomer = updateRow(originalCustomer, c_cid, 0L, null);
        // Propagate change to customer 22's orders and items to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, null, customerRT, 22L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, customerRT, 22L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 2L, customerRT, 23L));
        KeyUpdateRow updatedCustomer = updateRow(originalCustomer, c_cid, 11L, null);
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
        KeyUpdateRow originalVendor = testStore.find(new HKey(vendorRT, 1L));
        KeyUpdateRow updatedVendor = updateRow(originalVendor, v_vid, 0L, null);
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
        KeyUpdateRow originalVendorRow = testStore.find(new HKey(vendorRT, 1L));
        KeyUpdateRow updatedVendorRow = updateRow(originalVendorRow, v_vid, 2L, null);
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
        KeyUpdateRow itemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L, itemRT, 2222L));
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
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        KeyUpdateRow orderRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L));
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, customerRT, null, orderRT, 222L, itemRT, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L));
        // Propagate change to customer's descendents to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, customerRT, 22L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, null, customerRT, 22L, orderRT, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, customerRow));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, customerRT, 22L, orderRT, oid, itemRT, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
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
        KeyUpdateRow vendorRow = testStore.find(new HKey(vendorRT, 1L));
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
        vendorRT = getRowType(vendorId);
        customerRT = getRowType(customerId);
        orderRT = getRowType(orderId);
        itemRT = getRowType(itemId);
        group = customerRT.table().getGroup();
    }

    @Override
    protected List<List<Object>> vendorPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, vendorRT, v_vid);
    }

    @Override
    protected List<List<Object>> customerPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, customerRT, c_cid, c_vid);
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, o_oid, HKeyElement.from(1), o_cid);
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, itemRT, i_iid, HKeyElement.from(1), HKeyElement.from(3), i_oid);
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, o_priority, HKeyElement.from(1), o_cid, o_oid);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, o_when, HKeyElement.from(1), o_cid, o_oid);
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
        KeyUpdateRow customer;
        KeyUpdateRow order;
        //                               HKey reversed, value
        // Vendor 1
        dbInsert(             kurow(vendorRT, 1L, 100L));
        //
        dbInsert(customer =   kurow(customerRT, 11L, 1L, 1100L));
        dbInsert(order =      row(customer, orderRT,                111L, 11L,       11100L,   81L, 9001L));
        dbInsert(             row(order, customer, itemRT,    1111L, 111L,           111100L));
        dbInsert(             row(order, customer, itemRT,    1112L, 111L,           111200L));
        dbInsert(             row(order, customer, itemRT,    1113L, 111L,           111300L));
        dbInsert(order =      row(customer, orderRT,                112L, 11L,       11200L,   83L, 9002L));
        dbInsert(             row(order, customer, itemRT,    1121L, 112L,           112100L));
        dbInsert(             row(order, customer, itemRT,    1122L, 112L,           112200L));
        dbInsert(             row(order, customer, itemRT,    1123L, 112L,           112300L));
        dbInsert(order =      row(customer, orderRT,                113L, 11L,       11300L,   81L, 9003L));
        dbInsert(             row(order, customer, itemRT,    1131L, 113L,           113100L));
        dbInsert(             row(order, customer, itemRT,    1132L, 113L,           113200L));
        dbInsert(             row(order, customer, itemRT,    1133L, 113L,           113300L));

        dbInsert(customer =   kurow(customerRT, 12L, 1L, 1200L));
        dbInsert(order =      row(customer, orderRT,                121L, 12L,       12100L,   83L, 9004L));
        dbInsert(             row(order, customer, itemRT,    1211L, 121L,           121100L));
        dbInsert(             row(order, customer, itemRT,    1212L, 121L,           121200L));
        dbInsert(             row(order, customer, itemRT,    1213L, 121L,           121300L));
        dbInsert(order =      row(customer, orderRT,                122L, 12L,       12200L,   81L, 9005L));
        dbInsert(             row(order, customer, itemRT,    1221L, 122L,           122100L));
        dbInsert(             row(order, customer, itemRT,    1222L, 122L,           122200L));
        dbInsert(             row(order, customer, itemRT,    1223L, 122L,           122300L));
        dbInsert(order =      row(customer, orderRT,                123L, 12L,       12300L,   82L, 9006L));
        dbInsert(             row(order, customer, itemRT,    1231L, 123L,           123100L));
        dbInsert(             row(order, customer, itemRT,    1232L, 123L,           123200L));
        dbInsert(             row(order, customer, itemRT,    1233L, 123L,           123300L));

        dbInsert(customer =   kurow(customerRT, 13L, 1L, 1300L));
        dbInsert(order =      row(customer, orderRT,                131L, 13L,       13100L,   82L, 9007L));
        dbInsert(             row(order, customer, itemRT,    1311L, 131L,           131100L));
        dbInsert(             row(order, customer, itemRT,    1312L, 131L,           131200L));
        dbInsert(             row(order, customer, itemRT,    1313L, 131L,           131300L));
        dbInsert(order =      row(customer, orderRT,                132L, 13L,       13200L,   83L, 9008L));
        dbInsert(             row(order, customer, itemRT,    1321L, 132L,           132100L));
        dbInsert(             row(order, customer, itemRT,    1322L, 132L,           132200L));
        dbInsert(             row(order, customer, itemRT,    1323L, 132L,           132300L));
        dbInsert(order =      row(customer, orderRT,                133L, 13L,       13300L,   81L, 9009L));
        dbInsert(             row(order, customer, itemRT,    1331L, 133L,           133100L));
        dbInsert(             row(order, customer, itemRT,    1332L, 133L,           133200L));
        dbInsert(             row(order, customer, itemRT,    1333L, 133L,           133300L));
        //
        // Vendor 2
        dbInsert(             kurow(vendorRT, 2L, 200L));
        //
        dbInsert(customer =   kurow(customerRT, 21L, 2L, 2100L));
        dbInsert(order =      row(customer, orderRT,                211L, 21L,       21100L,   81L, 9010L));
        dbInsert(             row(order, customer, itemRT,    2111L, 211L,           211100L));
        dbInsert(             row(order, customer, itemRT,    2112L, 211L,           211200L));
        dbInsert(             row(order, customer, itemRT,    2113L, 211L,           211300L));
        dbInsert(order =      row(customer, orderRT,                212L, 21L,       21200L,   83L, 9011L));
        dbInsert(             row(order, customer, itemRT,    2121L, 212L,           212100L));
        dbInsert(             row(order, customer, itemRT,    2122L, 212L,           212200L));
        dbInsert(             row(order, customer, itemRT,    2123L, 212L,           212300L));
        dbInsert(order =      row(customer, orderRT,                213L, 21L,       21300L,   82L, 9012L));
        dbInsert(             row(order, customer, itemRT,    2131L, 213L,           213100L));
        dbInsert(             row(order, customer, itemRT,    2132L, 213L,           213200L));
        dbInsert(             row(order, customer, itemRT,    2133L, 213L,           213300L));

        dbInsert(customer =   kurow(customerRT, 22L, 2L, 2200L));
        dbInsert(order =      row(customer, orderRT,                221L, 22L,       22100L,   82L, 9013L));
        dbInsert(             row(order, customer, itemRT,    2211L, 221L,           221100L));
        dbInsert(             row(order, customer, itemRT,    2212L, 221L,           221200L));
        dbInsert(             row(order, customer, itemRT,    2213L, 221L,           221300L));
        dbInsert(order =      row(customer, orderRT,                222L, 22L,       22200L,   82L, 9014L));
        dbInsert(             row(order, customer, itemRT,    2221L, 222L,           222100L));
        dbInsert(             row(order, customer, itemRT,    2222L, 222L,           222200L));
        dbInsert(             row(order, customer, itemRT,    2223L, 222L,           222300L));
        dbInsert(order =      row(customer, orderRT,                223L, 22L,       22300L,   81L, 9015L));
        dbInsert(             row(order, customer, itemRT,    2231L, 223L,           223100L));
        dbInsert(             row(order, customer, itemRT,    2232L, 223L,           223200L));
        dbInsert(             row(order, customer, itemRT,    2233L, 223L,           223300L));

        dbInsert(customer =   kurow(customerRT, 23L, 2L, 2300L));
        dbInsert(order =      row(customer, orderRT,                231L, 23L,       23100L,   82L, 9016L));
        dbInsert(             row(order, customer, itemRT,    2311L, 231L,           231100L));
        dbInsert(             row(order, customer, itemRT,    2312L, 231L,           231200L));
        dbInsert(             row(order, customer, itemRT,    2313L, 231L,           231300L));
        dbInsert(order =      row(customer, orderRT,                232L, 23L,       23200L,   83L, 9017L));
        dbInsert(             row(order, customer, itemRT,    2321L, 232L,           232100L));
        dbInsert(             row(order, customer, itemRT,    2322L, 232L,           232200L));
        dbInsert(             row(order, customer, itemRT,    2323L, 232L,           232300L));
        dbInsert(order =      row(customer, orderRT,                233L, 23L,       23300L,   81L, 9018L));
        dbInsert(             row(order, customer, itemRT,    2331L, 233L,           233100L));
        dbInsert(             row(order, customer, itemRT,    2332L, 233L,           233200L));
        dbInsert(             row(order, customer, itemRT,    2333L, 233L,           233300L));
    }

    @Override
    protected HKey hKey(KeyUpdateRow row)
    {
        HKey hKey = null;
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            hKey = new HKey(vendorRT, row.value(v_vid).getInt64());
        } else if (rowType == customerRT) {
            hKey = new HKey(vendorRT, row.value(c_vid).getInt64(), 
                            customerRT, row.value(c_cid).getInt64());
        } else if (rowType == orderRT) {
            assertNotNull(row.parent());
            hKey = new HKey(vendorRT, row.parent().value(c_vid).getInt64(),
                            customerRT, row.value(o_cid).getInt64(),
                            orderRT, row.value(o_oid).getInt64());
        } else if (rowType == itemRT) {
            assertNotNull(row.parent());
            assertNotNull(row.parent().parent());
            hKey = new HKey(vendorRT, row.parent().parent().value(c_vid).getInt64(),
                            customerRT, row.parent().value(o_cid).getInt64(),
                            orderRT, row.value(i_oid).getInt64(),
                            itemRT, row.value(i_iid).getInt64());
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
    protected HKey hKey(KeyUpdateRow row, KeyUpdateRow parent, KeyUpdateRow grandparent)
    {
        HKey hKey = null;
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            hKey = new HKey(vendorRT, row.value(v_vid).getInt64());
        } else if (rowType == customerRT) {
            hKey = new HKey(vendorRT, row.value(c_vid).getInt64(),
                            customerRT, row.value(c_cid).getInt64());
        } else if (rowType == orderRT) {
            hKey = new HKey(vendorRT, parent == null ? null : parent.value(c_vid).getInt64(),
                            customerRT, row.value(o_cid).getInt64(),
                            orderRT, row.value(o_oid).getInt64());
        } else if (rowType == itemRT) {
            hKey = new HKey(vendorRT, grandparent == null ? null : grandparent.value(c_vid).getInt64(),
                            customerRT, parent == null ? null : parent.value(o_cid).getInt64(),
                            orderRT, row.value(i_oid).getInt64(),
                            itemRT, row.value(i_iid).getInt64());
        } else {
            fail();
        }
        row.parent(parent);
        return hKey;
    }

    protected void confirmColumns()
    {
        confirmColumn(vendorRT, v_vid, "vid");
        confirmColumn(vendorRT, v_vx, "vx");

        confirmColumn(customerRT, c_cid, "cid");
        confirmColumn(customerRT, c_vid, "vid");
        confirmColumn(customerRT, c_cx, "cx");

        confirmColumn(orderRT, o_oid, "oid");
        confirmColumn(orderRT, o_cid, "cid");
        confirmColumn(orderRT, o_ox, "ox");
        confirmColumn(orderRT, o_priority, "priority");
        confirmColumn(orderRT, o_when, "when");

        confirmColumn(itemRT, i_iid, "iid");
        confirmColumn(itemRT, i_oid, "oid");
        confirmColumn(itemRT, i_ix, "ix");
    }
}
